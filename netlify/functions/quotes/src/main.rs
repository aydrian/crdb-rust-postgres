use aws_lambda_events::encodings::Body;
use aws_lambda_events::event::apigw::{ApiGatewayProxyRequest, ApiGatewayProxyResponse};
use http::header::HeaderMap;
use lambda_runtime::{handler_fn, Context, Error};
use log::LevelFilter;
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use serde::Serialize;
use simple_logger::SimpleLogger;
use tokio_postgres::Client;

#[derive(Debug, Serialize)]
struct Quote {
    quote: Option<String>,
    characters: Option<String>,
    stardate: Option<rust_decimal::Decimal>,
    episode: Option<i64>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();

    let func = handler_fn(my_handler);
    lambda_runtime::run(func).await?;
    Ok(())
}

async fn my_handler(
    event: ApiGatewayProxyRequest,
    _ctx: Context,
) -> Result<ApiGatewayProxyResponse, Error> {
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
            "SELECT quote, characters, stardate, episode FROM quotes ORDER BY episode asc LIMIT 20;",
            &[],
        )
        .await?
    {
        let quote = Quote {
            quote: row.get(0),
            characters: row.get(1),
            stardate: row.get(2),
            episode: row.get(3),
        };
        quotes.push(quote);
    }

    Ok(quotes)
}
