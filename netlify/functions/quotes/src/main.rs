use aws_lambda_events::encodings::Body;
use aws_lambda_events::event::apigw::{ApiGatewayProxyRequest, ApiGatewayProxyResponse};
use http::header::HeaderMap;
use lambda_runtime::{handler_fn, Context, Error};
use log::LevelFilter;
use openssl::ssl::{SslConnector, SslMethod};
// use native_tls::{Certificate, TlsConnector};
// use postgres::Client;
use postgres_openssl::MakeTlsConnector;
// use postgres_native_tls::MakeTlsConnector;
use serde::Serialize;
use simple_logger::SimpleLogger;

#[derive(Debug, Serialize)]
struct Quote {
    quote: String,
    characters: String,
    episode: i64,
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
    _event: ApiGatewayProxyRequest,
    _ctx: Context,
) -> Result<ApiGatewayProxyResponse, Error> {
    let database_url = std::env::var("DATABASE_URL").expect("Must have a DATABASE_URL set");
    // let method = event.http_method;
    // let path = event.path.unwrap();

    let cert = std::fs::read("../cc-ca.crt")?;
    let cert = openssl::x509::X509::from_pem(&cert).unwrap();
    let mut ctx = SslConnector::builder(SslMethod::tls())?;
    ctx.set_certificate(&cert)?;

    let connector = MakeTlsConnector::new(ctx.build());
    //let cert = Certificate::from_pem(&cert)?;
    //let connector = TlsConnector::builder().add_root_certificate(cert).build()?;
    //let connector = MakeTlsConnector::new(connector);

    // let mut client = Client::connect(
    //     &database_url,
    //     connector,
    // )?;
    let (client, connection) = tokio_postgres::connect(&database_url, connector).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let mut quotes = Vec::new();

    for row in client.query("SELECT * FROM quotes LIMIT 5", &[]).await? {
        let quote = Quote {
            quote: row.get(0),
            characters: row.get(1),
            episode: row.get(3),
        };
        quotes.push(quote);
    }

    let json_quotes = serde_json::to_string(&quotes)?;

    let resp = ApiGatewayProxyResponse {
        status_code: 200,
        headers: HeaderMap::new(),
        multi_value_headers: HeaderMap::new(),
        body: Some(Body::Text(json_quotes)),
        is_base64_encoded: Some(false),
    };

    Ok(resp)
}
