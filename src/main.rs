use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer};
use actix_cors::Cors;
extern crate redis;
use std::collections::HashMap;

use futures::future::{BoxFuture, FutureExt};
use futures::stream::StreamExt;

use bson::{DateTime,bson};
use chrono::prelude::*;
use serde_json::{from_value, json};
use std::ops::Deref;
use mongodb::bson::{doc, Bson};
use mongodb::error::Error;
use mongodb::options::UpdateOptions;
use mongodb::{options::ClientOptions, Client, Collection};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Debug;
use std::fs;
use std::sync::{Arc, RwLock, Mutex};

#[derive(Serialize, Deserialize, Debug)]
struct Item {
    name: String,
    url: String,
    tokens: Option<i16>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionItem {
    pub collection: String,
    pub data: Vec<Marketplaces>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Marketplaces {
    pub marketplace: String,
    pub data: Vec<DataItem>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataItem {
    pub price: f64,
    pub time: DateTime,
    pub number_of_owners: u64,
    pub number_of_tokens_listed: usize,
    pub number_of_nft_per_owner: Bson,
    pub avrg_price: f64,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FetchAPI {
    pub owners: Vec<String>,
    pub prices: Vec<f64>,
}

async fn index(req: HttpRequest, client: web::Data<Conn>) -> HttpResponse {
    let mut collection = None;
    if let Some(content_type) = get_content_type(&req) {
        println!("1 - {}", content_type);
        format!("Got content-type = '{}'", content_type);
        collection = Some(content_type);
    } else {
        "No id header found".to_string();
    };

    let data = client.get_collection(collection.unwrap()).await;

    match data {
        Ok(value) => match value {
            Some(value) => HttpResponse::Ok().json(value.data),
            None => HttpResponse::NotFound().await.unwrap(),
        },
        Err(_) => HttpResponse::NotFound().await.unwrap(),
    }
}

fn get_content_type<'a>(req: &'a HttpRequest) -> Option<&'a str> {
    req.headers().get("id")?.to_str().ok()
}

async fn index_all_vaults(_req: HttpRequest, client: web::Data<Conn>) -> HttpResponse {
    let data = dbg!(client.get_redis_data("loadall").await);
    match data {
        Ok(value) => {
            
            let coll_item: serde_json::Value = dbg!(serde_json::from_str(&value).unwrap_or_default());
            println!("a - {:#?}",coll_item);
            HttpResponse::Ok().json(coll_item)
        },
        Err(_) => {

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
    }

    
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let conn = Conn::new().await;
    
    // conn.get_all_collections().await;
    save_de(&conn).await;
    save_so(&conn).await;

    HttpServer::new(move || {
        let cors = Cors::default()
            //   .allowed_origin("http://localhost:3000")
              .allow_any_origin()
              .allow_any_header()
              .allow_any_method()
            //   .expose_headers(headers)
            //   .allowed_methods(vec!["GET", "POST"])
       
              ;
        App::new()
            .app_data(web::Data::new(conn.clone()))
            .wrap(cors)
            .wrap(middleware::Logger::default())
            .service(web::resource("/load").to(index))
            .service(web::resource("/loadall").to(index_all_vaults))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

async fn save_de(conn: &Conn) {
    let path = "./src/collectionsDigitalEyes.json";
    let data = fs::read_to_string(path).expect("Unable to read file");
    let res: Vec<Item> = serde_json::from_str(&data).expect("Unable to parse");

    let next_cursor = "";
    for collection in &res {
        let de_data = Arc::new(RwLock::new(FetchAPI {
            owners: Vec::new(),
            prices: Vec::new(),
        }));

        let api_data = fetch_de(&collection.url, next_cursor, de_data)
            .await
            .unwrap();

        println!("{:?}\n", api_data);

        let mut owners: HashMap<String, u32> = HashMap::new();

        for owner in api_data.owners {
            if let Some(x) = owners.get_mut(&owner) {
                *x += 1;
            } else {
                owners.insert(owner.to_string(), 1);
            }
        }
        let mut number_of_nft_per_owner: HashMap<u32, u32> = HashMap::new();
        for nft in owners.values() {
            if let Some(x) = number_of_nft_per_owner.get_mut(&nft) {
                *x += 1;
            } else {
                number_of_nft_per_owner.insert(*nft, 1);
            }
        }
        let number_of_tokens_listed = api_data.prices.len();
        let avrg_price: f64 =
            &api_data.prices.iter().sum() / 1000000000.0 / number_of_tokens_listed as f64;
        let floor_price: f64 =
            &api_data.prices.into_iter().reduce(f64::min).unwrap() / 1000000000.0;

        let number_of_nft_per_owner = serde_json::to_string_pretty(&number_of_nft_per_owner).unwrap().to_string();
        let number_of_nft_per_owner: Value = serde_json::from_str(&number_of_nft_per_owner).unwrap();
        let number_of_nft_per_owner = Bson::try_from(number_of_nft_per_owner).unwrap();

        println!("****  {:?}", &number_of_nft_per_owner);

        let data_to_update = DataItem {
            price: floor_price,
            time: DateTime::from_chrono(Utc::now()),
            number_of_owners: owners.keys().len() as u64,
            number_of_tokens_listed,
            number_of_nft_per_owner,
            avrg_price,
        };
        

        conn.update_collection(
            data_to_update,
            "de".to_string(),
            collection.name.to_string(),
        )
        .await;
    }
}

fn fetch_de(
    url: &str,
    next_cursor: &str,
    de_data: Arc<RwLock<FetchAPI>>,
) -> BoxFuture<'static, Option<FetchAPI>> {
    let url = String::from(url);
    let next_cursor = next_cursor.to_string();
    async move {
        let mut next_cursor_v = "";
        // let one_million: u64 = 1000000000;
        let digitaleyes_url =
            "https://us-central1-digitaleyes-prod.cloudfunctions.net/offers-retriever?collection=";

        let body = reqwest::get(format!(
            "{}{}&cursor={}",
            digitaleyes_url.to_owned(),
            url,
            next_cursor
        ))
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
        
        let mut de_data_mut = de_data.read().unwrap().clone();

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
                        let nft_price = data.as_f64().unwrap();

                        if nft_price > 0.0 {
                            de_data_mut.prices.push(nft_price);
                        };
                    };
                    let nft = item.get("owner").unwrap();
                    if let Value::String(owner) = &nft {
                        if owner.len() > 0 {
                            de_data_mut.owners.push(owner.to_string());
                        };
                    }
                }
            }
        };
        // let x = *de_data.read().unwrap();
        if next_cursor_v.is_empty() {
            return Some(de_data_mut);
        } else {
            return fetch_de(&url, next_cursor_v, Arc::new(RwLock::new(de_data_mut))).await;
        };
    }
    .boxed()
}

async fn save_so(conn: &Conn) {
    let path = "./src/collectionsSolanart.json";
    let data = fs::read_to_string(path).expect("Unable to read file");
    let res: Vec<Item> = serde_json::from_str(&data).expect("Unable to parse");

    
    for collection in &res {
        let so_data = Arc::new(RwLock::new(FetchAPI {
            owners: Vec::new(),
            prices: Vec::new(),
        }));
        
        

        let api_data = fetch_so(&collection.url, so_data)
            .await.unwrap();
                        
        
        println!("{:?}\n", api_data);

        let mut owners: HashMap<String, u32> = HashMap::new();

        for owner in api_data.owners {
            if let Some(x) = owners.get_mut(&owner) {
                *x += 1;
            } else {
                owners.insert(owner.to_string(), 1);
            }
        }
        let mut number_of_nft_per_owner: HashMap<u32, u32> = HashMap::new();
        for nft in owners.values() {
            if let Some(x) = number_of_nft_per_owner.get_mut(&nft) {
                *x += 1;
            } else {
                number_of_nft_per_owner.insert(*nft, 1);
            }
        }
        let number_of_tokens_listed = api_data.prices.len();
        let avrg_price: f64 =
            &api_data.prices.iter().sum() / number_of_tokens_listed as f64;
        let floor_price: f64 =
            api_data.prices.into_iter().reduce(f64::min).unwrap() ;

        let number_of_nft_per_owner = serde_json::to_string_pretty(&number_of_nft_per_owner).unwrap().to_string();
        let number_of_nft_per_owner: Value = serde_json::from_str(&number_of_nft_per_owner).unwrap();
        let number_of_nft_per_owner = Bson::try_from(number_of_nft_per_owner).unwrap();

        println!("****  {:?}", &number_of_nft_per_owner);

        let data_to_update = DataItem {
            price: floor_price,
            time: DateTime::from_chrono(Utc::now()),
            number_of_owners: owners.keys().len() as u64,
            number_of_tokens_listed,
            number_of_nft_per_owner,
            avrg_price,
        };
        
        conn.update_collection(
            data_to_update,
            "so".to_string(),
            collection.name.to_string(),
        )
        .await;
    }
}

fn fetch_so(
    url: &str,
    so_data: Arc<RwLock<FetchAPI>>,
) -> BoxFuture<'static, Option<FetchAPI>> {
    let url = String::from(url);
   
    async move {
        
        let solanart_url =
        "https://api.solanart.io/get_nft?collection=";

        let body = reqwest::get(format!(
            "{}{}&page=1&limit=99999999&order=price-ASC&min=0&max=99999&search=&listed=true&fits=all&bid=all",
            solanart_url.to_owned(),
            url
       
        ))
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
        
        let mut so_data_mut = so_data.read().unwrap().clone();

        if let Value::Object(coll) = &body {
            // El body es de la variant object
            let items = coll.get("items").unwrap();
           
            if let Value::Array(offers) = &items {
                for item in offers {
                    let nft = item.get("price").unwrap();
                    if let Value::Number(data) = &nft {
                        let nft_price = data.as_f64().unwrap();

                        if nft_price > 0.0 {
                            so_data_mut.prices.push(nft_price);
                        };
                    };
                    let nft = item.get("seller_address").unwrap();
                    if let Value::String(owner) = &nft {
                        if owner.len() > 0 {
                            so_data_mut.owners.push(owner.to_string());
                        };
                    }
                }
            }
        };
       
       
            return Some(so_data_mut);
        
    }
    .boxed()
}

#[derive(Clone)]
struct Conn {
    
    mongo_db: Collection<CollectionItem>,
    redis_conn: redis::aio::MultiplexedConnection
    
}

impl Conn {
    // constructor
    pub async fn new() -> Self {
        let client_options = ClientOptions::parse("mongodb://localhost:27017")
            .await
            .unwrap();

        let client = Client::with_options(client_options).unwrap();
        let mongo_db = client
            .database("floorprice")
            .collection::<CollectionItem>("datafetcheds");
        let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let redis_conn = redis_client.get_multiplexed_tokio_connection().await.unwrap();
       
    
        Self {  mongo_db, redis_conn }
    }

    pub async fn get_collection(&self, name: &str) -> Result<Option<CollectionItem>, Error> {
        println!("coll: {name}");
        dbg!(
            self.mongo_db
                .find_one(
                    doc! {
                        "collection": name.to_owned()
                    },
                    None,
                )
                .await
        )
    }

    pub async fn get_redis_data(&self, key:&str) -> Result<String, redis::RedisError> {
        let mut redis_conn = self.redis_conn.clone();

        let res:String = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut redis_conn).await?;

        
        println!("redis get {}",res);
        Ok(res)
        
        
    }
    pub async fn get_all_collections(&self) -> Result<Vec<CollectionItem>, Error> {
        
        let mut collections = Vec::new();
        let query = vec![
            doc! {
                "$unwind": {
                    "path": "$data"
                }
            },
            doc! {
                "$unwind": {
                    "path": "$data.marketplace"
                }
            },
            doc! {
                "$sort": {
                    "data.data.time": -1
                }
            },
            doc! {
                "$set": {
                    "collection": "$collection",
                    "data.data": {
                        "$last": "$data.data"
                    }
                }
            },
            doc! {
                "$group": {
                    "_id": "$_id",
                    "collection": {
                        "$first": "$collection"
                    },
                    "data": {
                        "$addToSet": "$data"
                    }
                }
            },
            doc! {
                "$unwind": {
                    "path": "$data"
                }
            },
            doc! {
                "$set": {
                    "data.data": [
                        "$data.data"
                    ]
                }
            },
            doc! {
                "$group": {
                    "_id": "$_id",
                    "collection": {
                        "$first": "$collection"
                    },
                    "data": {
                        "$addToSet": "$data"
                    }
                }
            },
        ];
        println!("pre find");

        let mut data = self.mongo_db.aggregate(query, None).await?;
        let mut collections_redis = Vec::new();
        
        while let Some(Ok(doc)) = data.next().await {
            let x = format!("{},",serde_json::to_string(&doc).unwrap());
            println!("x - {}",x);
            collections_redis.push(x);
            let coll_item: CollectionItem = bson::from_document(doc).unwrap();
            
            collections.push(coll_item);
        }

        collections_redis.last_mut().unwrap().pop().unwrap();


        let collections_redis = format!("[{}]",collections_redis.join(""));

        
        
        let mut redis_conn = self.redis_conn.clone();
        
        let _: () = redis::cmd("SETEX")
        .arg("loadall")
        .arg(1800 as usize)
        .arg(&collections_redis)
        .query_async(&mut redis_conn).await.unwrap_or_default();

        Ok(collections)
    }

    pub async fn update_collection(&self, data: DataItem, marketplace: String, name: String) {
        let filter = doc! {"collection": &name, "data.marketplace":&marketplace};
        let options = UpdateOptions::builder().upsert(Some(true)).build();
        let update = doc! {
            "$push":{
                "data.$.data":{
                    "price":  Bson::Double(data.price),
                    "time": data.time,
                    "number_of_owners": Bson::Int32(data.number_of_owners as i32),
                    "number_of_tokens_listed": Bson::Int32(data.number_of_tokens_listed as i32),
                    "number_of_nft_per_owner": &data.number_of_nft_per_owner,
                    "avrg_price": Bson::Double(data.avrg_price as f64)
                }
            }
        };
        let res = self
            .mongo_db
            .update_one(filter, update, Some(options))
            .await;

        if res.is_err() {
            let filter = doc! {"collection": &name};
            let options = UpdateOptions::builder().upsert(Some(true)).build();
            
            let update = doc! {
                "$push":{
                    "data":{
                        "marketplace":&marketplace.to_owned(),
                        "data":[{
                        "price":  Bson::Double(data.price),
                        "time": data.time,
                        "number_of_owners": Bson::Int32(data.number_of_owners as i32),
                        "number_of_tokens_listed": Bson::Int32(data.number_of_tokens_listed as i32),
                        "number_of_nft_per_owner": &data.number_of_nft_per_owner,
                        "avrg_price": Bson::Double(data.avrg_price as f64)
                        }]
                    }
                }
            };
            let _res = self.mongo_db.update_one(filter, update, options).await;
        }
    }
}
