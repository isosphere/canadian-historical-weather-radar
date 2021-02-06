const IMAGE_BASE_URL: &str = "https://climate.weather.gc.ca/radar/image_e.html";

extern crate chrono;

#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate slog_async;
extern crate ureq;

use chrono::{Duration, TimeZone, Utc};
use clap::{Arg, App};
use indicatif::ProgressBar;
use slog::Drain;
use std::fs::File;
use std::io::prelude::*;
use std::io::Read;
use std::path::Path;
use rayon::prelude::*;
use ureq::Error;

fn command_usage<'a, 'b>() -> App<'a, 'b> {
    const DEFAULT_START_HOUR: &str = "0";

    App::new("data-acquisition")
    .author("Matthew Scheffel <matt@dataheck.com>")
    .about("Downloads historical weather radar images from Environment and Climate Change Canada")
    .arg(
        Arg::with_name("site")
            .short("s")
            .long("site")
            .takes_value(true)
            .required(true)
            .help("Which site to pull data for. List as of this writing: CASBI, CASCM, CASFT, CASGO, CASKR, CASLC, CASLA, CASBV, CASVD, CASSF. Aggregations are available as NAT, PYR, PNR, ONT, QUE, and ATL.")
    )
    .arg(
        Arg::with_name("image-type")
            .long("image-type")
            .takes_value(true)
            .required(true)
            .help("What kind of image type to request. Examples: PERCIP_SNOW_WEATHEROFFICE, PERCIP_RAIN_WEATHEROFFICE")
    )
    .arg(
        Arg::with_name("start-year")
            .long("start-year")
            .takes_value(true)
            .required(true)
            .help("Collection will start with this year")
    )
    .arg(
        Arg::with_name("end-year")
            .long("end-year")
            .takes_value(true)
            .required(true)
            .help("Collection will end with this year")
    )
    .arg(
        Arg::with_name("start-month")
            .long("start-month")
            .takes_value(true)
            .required(true)
            .help("Collection will start with this month (numeric, 1-12)")
    )
    .arg(
        Arg::with_name("end-month")
            .long("end-month")
            .takes_value(true)
            .required(true)
            .help("Collection will end with this month (numeric, 1-12)")
    )
    .arg(
        Arg::with_name("start-day")
            .long("start-day")
            .takes_value(true)
            .required(true)
            .help("Collection will start with this day")
    )
    .arg(
        Arg::with_name("end-day")
            .long("end-day")
            .takes_value(true)
            .required(true)
            .help("Collection will end with this day")
    )
    .arg(
        Arg::with_name("start-hour")
            .long("start-hour")
            .takes_value(true)
            .default_value(DEFAULT_START_HOUR)
            .help("Collection will start with this hour")
    )
    .arg(
        Arg::with_name("directory")
            .long("directory")
            .takes_value(true)
            .required(true)
            .help("Where the downloaded images should be stored. Directory will be created if it does not exist. If the directory does exist, the software will not download existing files.")
    )
}

fn process_file(file_url: &str, directory: &str, identifier: &str) -> Result<(), ()> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let log = slog::Logger::root(drain, o!());

    let file_processor = log.new(o!("file_url" => file_url.to_owned()));

    match ureq::get(file_url).call() {
        Ok(response) => {
            if !Path::new(directory).exists() {
                std::fs::create_dir(directory).expect("Failed to create specified directory, which does not exist.");
            }

            let concat = format!("{directory}/{identifier}", directory=directory, identifier=identifier);
            let path = Path::new(&concat);

            let mut bytes = Vec::new();
            response.into_reader().read_to_end(&mut bytes).expect("Failed to process response from server as an array of bytes.");
            if !bytes.is_empty() {
                let mut file = match File::create(path) {
                    Ok(f) => { f },
                    Err(err) => {
                        error!(file_processor, "Failed to create file due to error: '{}'", err);
                        return Err(());
                    }
                };
                file.write_all(&bytes).expect("Failed to write bytes to file.");
                Ok(())
            } else {
                Err(())
            }
        },
        Err(Error::Status(code, _)) => {
            error!(file_processor, "HTTP error code {} recieved when fetching url.", code);
            Err(())
        },
        Err(_) => {
            error!(file_processor, "I/O or transport error occured when fetching url.");
            Err(())
        }
    }
}

fn main() {
    let matches = command_usage().get_matches();

    let start_date = Utc.ymd(
        matches.value_of("start-year").unwrap().parse::<i32>().unwrap_or_else(|_| panic!("Invalid start-year specified.")),
        matches.value_of("start-month").unwrap().parse::<u32>().unwrap_or_else(|_| panic!("Invalid start-month specified.")), 
        matches.value_of("start-day").unwrap().parse::<u32>().unwrap_or_else(|_| panic!("Invalid start-day specified.")), 
    );

    let end_date = Utc.ymd(
        matches.value_of("end-year").unwrap().parse::<i32>().unwrap_or_else(|_| panic!("Invalid end-year specified.")),
        matches.value_of("end-month").unwrap().parse::<u32>().unwrap_or_else(|_| panic!("Invalid end-month specified.")), 
        matches.value_of("end-day").unwrap().parse::<u32>().unwrap_or_else(|_| panic!("Invalid end-day specified.")), 
    );

    let directory = matches.value_of("directory").unwrap();

    let existing_files = {
        if !Path::new(directory).exists() {
            std::fs::create_dir(directory).expect("Failed to create specified directory, which does not exist.");
            None
        } else {
            Some(
                std::fs::read_dir(directory).expect("Failed to read directory, though it exists.")
                .map(|res| res.map(|e| e.file_name().to_str().unwrap().to_owned()))
                .collect::<Result<Vec<_>, std::io::Error>>().expect("Failed to walk directory to find existing files.")
            )
        }
    };
    
    let mut file_urls = Vec::new();
    
    let mut dt = start_date;
    while dt <= end_date {
        for hour in 0 .. 23 { 
            let file_name = format!(
                "{site}_{imagetype}_{year}-{month}-{day}T{hour}-00.gif",
                year=dt.format("%Y"), month=dt.format("%m"), day=dt.format("%d"), hour=format!("{:02}", hour),
                site=matches.value_of("site").unwrap(), imagetype=matches.value_of("image-type").unwrap()
            );
            
            if let Some(file_list) = existing_files.as_ref() {
                if file_list.iter().any(|x| x == &file_name) {
                    continue;
                }
            }

            let fetch_url = format!(
                "{base}?time={year}{month}{day}{hour}00&site={site}&image_type={imagetype}", 
                base=IMAGE_BASE_URL, year=dt.format("%Y"), month=dt.format("%m"), day=dt.format("%d"),
                hour=format!("{:02}", hour), site=matches.value_of("site").unwrap(), imagetype=matches.value_of("image-type").unwrap()
            );
            file_urls.push((fetch_url, file_name ));
        }
        dt = dt + Duration::days(1);
    }

    let bar = ProgressBar::new(file_urls.len() as u64);

    let _results: Vec<Result<(), ()>> = file_urls.par_iter().map(
        |(path, identifier)| 
        { bar.inc(1); process_file(path, directory, identifier)}
    ).collect();

    bar.finish();
}
