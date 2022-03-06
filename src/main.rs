use actix_web::web::Data;
use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer};

use futures::future::{BoxFuture, FutureExt};
use futures::stream::{StreamExt, TryStreamExt};

use bson::DateTime;
use chrono::prelude::*;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{bson, doc, Bson, Document};
use mongodb::error::Error;
use mongodb::options::{FindOptions, UpdateOptions};
use mongodb::{options::ClientOptions, Client, Collection};
use serde::{Deserialize, Serialize};
use serde_json::{to_string, Value};
use std::fmt::Debug;
use std::fs;
use std::sync::{Arc, RwLock};

#[derive(Serialize, Deserialize, Debug)]
struct Item {
    name: String,
    url: String,
    tokens: Option<i16>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionItem {
    pub collection: String,
    pub data: Vec<DataItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionItemPipeline {
    pub collection: String,
    pub data: Document,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataItem {
    pub price: f64,
    pub time: DateTime,
    pub marketplace: String,
}

async fn index(_req: HttpRequest, client: web::Data<Conn>) -> HttpResponse {
    /*let solanart_url = "https://qzlsklfacc.medianetwork.cloud/nft_for_sale?collection=";

    let body = reqwest::get(solanart_url.to_string() + &"thugbirdz".to_string())
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // println!("body = {:?}", body);
    // println!("REQ: {:?}", req);
    body*/
    let data = client.get_collection("thugbirdz").await;

    match data {
        Ok(value) => match value {
            Some(value) => HttpResponse::Ok().json(value.data),
            None => HttpResponse::NotFound().await.unwrap(),
        },
        Err(_) => HttpResponse::NotFound().await.unwrap(),
    }
}

async fn index_all_vaults(_req: HttpRequest, client: web::Data<Conn>) -> HttpResponse {
    let data = client.get_all_collections().await;

    match data {
        Ok(value) => {
            let mut collections = Vec::new();
            for val in value {
                collections.push(val);
            }
            HttpResponse::Ok().json(collections)
        }

        Err(_) => HttpResponse::NotFound().await.unwrap(),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let conn = Conn::new().await;
    let client_ref = conn.clone();
    // conn.get_all_collections().await;
    // save_de(client_ref).await;

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(conn.clone()))
            .wrap(middleware::Logger::default())
            .service(web::resource("/vault").to(index))
            .service(web::resource("/allvaults").to(index_all_vaults))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

async fn save_de(conn: Conn) {
    let path = "./src/collectionsDigitalEyes.json";
    let data = fs::read_to_string(path).expect("Unable to read file");
    let res: Vec<Item> = serde_json::from_str(&data).expect("Unable to parse");

    let next_cursor = "";
    for collection in &res {
        let fp = Arc::new(RwLock::new(0));

        let floor_price: u64 = fetch_de(&collection.url, &next_cursor, fp).await.unwrap();
        println!("{:?}\n", collection.url);
        println!("price {:?}", floor_price);
        conn.update_collection(&collection.name, floor_price).await;
    }
}

fn fetch_de(url: &str, next_cursor: &str, fp: Arc<RwLock<u64>>) -> BoxFuture<'static, Option<u64>> {
    let url = url.to_string();
    let next_cursor = next_cursor.to_string();
    async move {
        let mut next_cursor_v = "";
        // let one_million: u64 = 1000000000;
        let digitaleyes_url =
            "https://us-central1-digitaleyes-prod.cloudfunctions.net/offers-retriever?collection=";

        let body = reqwest::get(format!(
            "{}{}&cursor={}",
            digitaleyes_url.to_string(),
            url,
            next_cursor
        ))
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();

        let mut price: Option<u64> = None;

        if *fp.read().unwrap() != 0 {
            price = Some(*fp.read().unwrap());
        }
        if let Value::Object(coll) = &body {
            // El body es de la variant object
            let num = coll.get("offers").unwrap();
            let next_cursor_value = coll.get("next_cursor").unwrap();

            if let Value::String(nextcursor) = &next_cursor_value {
                next_cursor_v = nextcursor;
            }
            if let Value::Array(offers) = &num {
                for item in offers {
                    let nft = item.get("price").unwrap();
                    if let Value::Number(data) = &nft {
                        let nft_price = data.as_u64().unwrap();

                        if price.is_none() {
                            price = Some(nft_price);
                        } else {
                            if nft_price < price.unwrap() {
                                price = Some(nft_price);
                            };
                        }
                    }
                }
            }
        }

        if next_cursor_v.is_empty() {
            Some(price.unwrap())
        } else {
            fetch_de(&url, next_cursor_v, Arc::new(RwLock::new(price.unwrap()))).await
        }
    }
    .boxed()
}

#[derive(Clone)]
struct Conn {
    client: Client,
    database: Collection<CollectionItem>,
}

impl Conn {
    // constructor
    pub async fn new() -> Self {
        let client_options = ClientOptions::parse("mongodb://localhost:27017")
            .await
            .unwrap();

        let client = Client::with_options(client_options).unwrap();
        let database = client
            .database("floorprice")
            .collection::<CollectionItem>("datafetcheds");

        Self { client, database }
    }

    pub async fn get_collection(&self, name: &str) -> Result<Option<CollectionItem>, Error> {
        self.database
            .find_one(
                doc! {
                    "collection": name.to_string()
                },
                None,
            )
            .await
    }

    pub async fn get_all_collections(&self) -> Result<Vec<CollectionItem>, Error> {
        let mut collections = Vec::new();
        let query = vec![
            doc! {
                "$sort": {
                    "data.time": -1
                }
            },
            doc! {
                "$unwind": {
                    "path": "$data"
                }
            },
            doc! {
                "$group": {
                    "_id": "$_id",
                    "data": {
                        "$last": "$data"
                    },
                    "collection": {
                        "$last": "$collection"
                    }
                }
            },
            doc! {
                "$set": {

                    "data": [
                        "$data"
                    ]
                }
            },
        ];
        println!("pre find");
        let mut data = self.database.aggregate(query, None).await?;

        while let Some(Ok(doc)) = data.next().await {
            println!("a {:#?}", doc);

            let coll_item: CollectionItem = bson::from_document(doc).unwrap();

            println!("x {:#?}", coll_item);
            collections.push(coll_item);
        }
        Ok(collections)
    }

    pub async fn update_collection(&self, name: &str, price: u64) {
        let filter = doc! {"collection": &name};
        let price = Bson::Double(price as f64 / 1000000000 as f64);
        let options = UpdateOptions::builder().upsert(Some(true)).build();

        let update = doc! {
            "$push":{
                "data":{
                    "price": &price,
                    "time":bson::Bson::DateTime(bson::DateTime::from_chrono(Utc::now())),
                    "marketplace":"de".to_string()
                },
            }
        };
        let res = self
            .database
            .update_one(filter, update, Some(options))
            .await
            .unwrap();
        println!("{:?}", res);
    }
}
