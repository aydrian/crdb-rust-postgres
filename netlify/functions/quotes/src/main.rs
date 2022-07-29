use aws_lambda_events::{
    encodings::Body,
    event::apigw::{ApiGatewayProxyRequest, ApiGatewayProxyResponse},
};
use http::header::HeaderMap;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use log::LevelFilter;
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use tokio_postgres::types::Type;
use tokio_postgres::Client;

#[derive(Debug, Serialize, Deserialize)]
struct Quote {
    id: Option<i64>,
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
            let quotes = get_quotes(client).await?;
            let json_quotes = serde_json::to_string(&quotes)?;

            ApiGatewayProxyResponse {
                status_code: 200,
                headers: HeaderMap::new(),
                multi_value_headers: HeaderMap::new(),
                body: Some(Body::Text(json_quotes)),
                is_base64_encoded: Some(false),
            }
        }
        http::Method::POST => {
            let new_quote: Quote = serde_json::from_str(&event.body.unwrap())?;
            let res = insert_quote(client, new_quote).await?;
            ApiGatewayProxyResponse {
                status_code: 201,
                headers: HeaderMap::new(),
                multi_value_headers: HeaderMap::new(),
                body: Some(Body::Text(res.to_string())),
                is_base64_encoded: Some(false),
            }
        }
        http::Method::DELETE => {
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
            id: row.get(0),
            quote: row.get(1),
            characters: row.get(2),
            stardate: row.get(3),
            episode: row.get(4),
        };
        quotes.push(quote);
    }

    Ok(quotes)
}

async fn insert_quote(client: Client, new_quote: Quote) -> Result<u64, Error> {
    let statement = client
        .prepare_typed(
            "INSERT INTO quotes (quote, characters, stardate, episode) VALUES ($1, $2, $3, $4);",
            &[Type::VARCHAR, Type::VARCHAR, Type::NUMERIC, Type::INT8],
        )
        .await?;

    let res = client
        .execute(
            &statement,
            &[
                &new_quote.quote,
                &new_quote.characters,
                &new_quote.stardate,
                &new_quote.episode,
            ],
        )
        .await?;

    Ok(res)
}

async fn delete_quote(client: Client, rowid: i64) -> Result<u64, Error> {
    let statement = client
        .prepare_typed("DELETE FROM quotes WHERE rowid = $1", &[Type::INT8])
        .await?;

    let res = client.execute(&statement, &[&rowid]).await?;

    Ok(res)
}
