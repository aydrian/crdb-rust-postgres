use aws_lambda_events::{
    encodings::Body,
    event::apigw::{ApiGatewayProxyRequest, ApiGatewayProxyResponse},
};
use http::header::HeaderMap;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use log::LevelFilter;
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
// use postgres_querybuilder::prelude::{QueryBuilder, QueryBuilderWithSet, QueryBuilderWithWhere};
use rust_decimal::Decimal;
use scooby::postgres::update;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use simple_logger::SimpleLogger;
use tokio_postgres::types::Type;
use tokio_postgres::Client;

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct Quote {
    #[serde_as(as = "Option<DisplayFromStr>")]
    rowid: Option<i64>,
    quote: Option<String>,
    characters: Option<String>,
    stardate: Option<Decimal>,
    episode: Option<i64>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();

    let processor = service_fn(handler);
    lambda_runtime::run(processor).await?;
    Ok(())
}

async fn handler(
    event: LambdaEvent<ApiGatewayProxyRequest>,
) -> Result<ApiGatewayProxyResponse, Error> {
    let (event, _context) = event.into_parts();
    let method = event.http_method;

    let client = get_db_client().await?;

    let resp = match method {
        http::Method::GET => {
            let json_resp = if let Some(rowid) = event.query_string_parameters.first("rowid") {
                // Handle not found error
                let quote = get_quote(client, rowid.parse::<i64>()?).await?;
                serde_json::to_string(&quote)?
            } else {
                let quotes = get_quotes(client).await?;
                serde_json::to_string(&quotes)?
            };

            ApiGatewayProxyResponse {
                status_code: 200,
                headers: HeaderMap::new(),
                multi_value_headers: HeaderMap::new(),
                body: Some(Body::Text(json_resp)),
                is_base64_encoded: Some(false),
            }
        }
        http::Method::POST => {
            // TODO: Return 400 if deserialize fails
            let new_quote: Quote = serde_json::from_str(&event.body.unwrap())?;
            let new_quote = insert_quote(client, new_quote).await?;
            let quote_json = serde_json::to_string(&new_quote)?;
            ApiGatewayProxyResponse {
                status_code: 201,
                headers: HeaderMap::new(),
                multi_value_headers: HeaderMap::new(),
                body: Some(Body::Text(quote_json)),
                is_base64_encoded: Some(false),
            }
        }
        http::Method::PUT => {
            // TODO: Return 400 if no rowid given
            let rowid: i64 = event
                .query_string_parameters
                .first("rowid")
                .unwrap()
                .parse()?;
            // TODO: Return 400 if deserialize fails
            let quote = serde_json::from_str(&event.body.unwrap())?;

            let res = update_quote(client, rowid, quote).await?;

            ApiGatewayProxyResponse {
                status_code: 201,
                headers: HeaderMap::new(),
                multi_value_headers: HeaderMap::new(),
                body: Some(Body::Text(res.to_string())),
                is_base64_encoded: Some(false),
            }
        }
        http::Method::DELETE => {
            // TODO: Return 400 if no rowid given
            let rowid: i64 = event
                .query_string_parameters
                .first("rowid")
                .unwrap()
                .parse()?;

            let _res = delete_quote(client, rowid).await?;

            ApiGatewayProxyResponse {
                status_code: 204,
                headers: HeaderMap::new(),
                multi_value_headers: HeaderMap::new(),
                body: Some(Body::Empty),
                is_base64_encoded: Some(false),
            }
        }
        _ => ApiGatewayProxyResponse {
            status_code: 405,
            headers: HeaderMap::new(),
            multi_value_headers: HeaderMap::new(),
            body: Some(Body::Text(String::from("Method Not Allowed"))),
            is_base64_encoded: Some(false),
        },
    };

    Ok(resp)
}

async fn get_db_client() -> Result<Client, Error> {
    let database_url = std::env::var("DATABASE_URL").expect("Must have a DATABASE_URL set");

    let cert = std::fs::read("../cc-ca.crt")?;
    let cert = openssl::x509::X509::from_pem(&cert).unwrap();
    let mut ctx = SslConnector::builder(SslMethod::tls())?;
    ctx.set_certificate(&cert)?;
    let connector = MakeTlsConnector::new(ctx.build());

    let (client, connection) = tokio_postgres::connect(&database_url, connector).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    return Ok(client);
}

async fn get_quotes(client: Client) -> Result<Vec<Quote>, Error> {
    let mut quotes = Vec::new();

    for row in client
        .query(
            "SELECT rowid, quote, characters, stardate, episode FROM quotes ORDER BY episode asc LIMIT 20;",
            &[],
        )
        .await?
    {
        let quote = Quote {
            rowid: row.get(0),
            quote: row.get(1),
            characters: row.get(2),
            stardate: row.get(3),
            episode: row.get(4),
        };
        quotes.push(quote);
    }

    Ok(quotes)
}

async fn get_quote(client: Client, rowid: i64) -> Result<Option<Quote>, tokio_postgres::Error> {
    let row = client
        .query_opt(
            "SELECT rowid, quote, characters, stardate, episode FROM quotes WHERE rowid=$1;",
            &[&rowid],
        )
        .await?;

    match row {
        Some(row) => {
            let quote = Quote {
                rowid: row.get(0),
                quote: row.get(1),
                characters: row.get(2),
                stardate: row.get(3),
                episode: row.get(4),
            };
            Ok(Some(quote))
        }
        None => Ok(None),
    }
}

async fn insert_quote(client: Client, new_quote: Quote) -> Result<Quote, Error> {
    let statement = client
        .prepare_typed(
            "INSERT INTO quotes (quote, characters, stardate, episode) VALUES ($1, $2, $3, $4) RETURNING rowid, quote, characters, stardate, episode;",
            &[Type::VARCHAR, Type::VARCHAR, Type::NUMERIC, Type::INT8],
        )
        .await?;

    let row = client
        .query_opt(
            &statement,
            &[
                &new_quote.quote,
                &new_quote.characters,
                &new_quote.stardate,
                &new_quote.episode,
            ],
        )
        .await?
        .unwrap();

    let quote = Quote {
        rowid: row.get(0),
        quote: row.get(1),
        characters: row.get(2),
        stardate: row.get(3),
        episode: row.get(4),
    };

    Ok(quote)
}

async fn update_quote(client: Client, rowid: i64, quote: Quote) -> Result<u64, Error> {
    let query = update("quote");
    let query = query.set("quote", quote.quote.unwrap());
    let query = query.where_(format!("rowid={}", rowid));

    let statement = client.prepare(&query.to_string()).await?;

    // let mut builder = postgres_querybuilder::UpdateBuilder::new("quotes");
    // if let Some(q) = quote.quote {
    //     builder.set("quote", q);
    // };
    // if let Some(q) = quote.episode {
    //     builder.set("episode", q);
    // };
    // if let Some(q) = quote.characters {
    //     builder.set("characters", q);
    // };
    // if let Some(q) = quote.stardate {
    //     builder.set("stardate", q.to_string());
    // };
    // builder.where_eq("rowid", rowid);
    // dbg!(&builder.get_query());
    // dbg!(&builder.get_ref_params());

    // let statement = client.prepare(&builder.get_query()).await?;

    let res = client.execute(&statement, &[]).await?;

    Ok(res)
}

async fn delete_quote(client: Client, rowid: i64) -> Result<u64, Error> {
    let statement = client
        .prepare_typed("DELETE FROM quotes WHERE rowid = $1", &[Type::INT8])
        .await?;

    let res = client.execute(&statement, &[&rowid]).await?;

    Ok(res)
}
