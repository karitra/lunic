use std::path::Path;

use std::fs::File;
use std::io::Read;

use yaml_rust::{Yaml, YamlLoader};

use cocaine::service::tvm::Grant;


pub const GATHER_INTERVAL_SECS: u64 = 60;


#[derive(Debug)]
pub struct Config {
    pub gather_interval: u64,
    pub ticket_expire_sec: Option<i64>,
    pub secure: Option<Secure>,
}

#[derive(Debug, Clone)]
pub struct Secure {
    pub md: String,
    pub client_id: i64,
    pub client_secret: String,
    pub grant: Option<Grant>,
}

#[derive(Debug)]
struct Builder {
    config: Config
}


fn yaml_from_file(path: &str) -> Option<Vec<Yaml>> {
    if !Path::new(path).is_file() {
        println!("file not exist {}", path);
        return None
    }

    if let Ok(mut fl) = File::open(path) {
        let mut content = String::new();
        match fl.read_to_string(&mut content) {
            Ok(_usize) =>  YamlLoader::load_from_str(&content).ok(),
            Err(_) => None
        }
    } else {
        None
    }
}


impl Config {
    fn new_with_defaults() -> Config {
        Config{
            gather_interval: GATHER_INTERVAL_SECS,
            ticket_expire_sec: Some(600),
            secure: None
        }
    }

    pub fn _new_from_default_files(files: &[&str]) -> Config {
        Self::new_from_files(files)
    }

    pub fn new_from_files(paths: &[&str]) -> Config {
        let mut builder = Builder::new();

        for file in paths {
            println!("checking for config: {}", file);

            if let Some(extension) = Path::new(file)
                .extension()
                .and_then(|ext| ext.to_str())
            {
                match extension {
                    "yaml" | "yml" =>
                        if let Some(yaml) = yaml_from_file(file) {
                            builder.update_from_yaml(yaml);
                        },
                    "toml" | "tml" => println!("toml format not implemented: {}", file),
                    _ => println!("unsupported config format: {}", file)
                };
            }
        }

        builder.build()
    }
}


impl Secure {
    pub fn get_mod(&self) -> String {
        self.md.clone()
    }
}


impl Builder {
    fn new() -> Builder {
        Builder{ config: Config::new_with_defaults() }
    }

    fn add_secure(&mut self, md: String, client_id: i64, client_secret: String, grant: Option<Grant>) -> &mut Self {
        self.config.secure = Some(Secure{md, client_id, client_secret, grant});
        self
    }

    fn build(self) -> Config {
        self.config
    }

    fn update_from_yaml(&mut self, yaml: Vec<Yaml>) -> &Self {
        fn str_to_yaml(s: &str) -> Yaml {
            Yaml::from_str(s)
        }

        for yaml in yaml {
            // update secure section
            yaml.as_hash()
                .and_then(|tb| tb.get(&str_to_yaml("secure")))
                .and_then(|tb| tb.as_hash())
                .and_then::<Yaml,_>(|tb| {
                    let md = tb.get(&str_to_yaml("mod")).and_then(|v| v.as_str());
                    let client_id = tb.get(&str_to_yaml("client_id")).and_then(|v| v.as_i64());
                    let client_secret = tb.get(&str_to_yaml("client_secret")).and_then(|v| v.as_str());

                    if let (Some(md), Some(client_id), Some(client_secret)) = (md, client_id, client_secret) {
                        let md = String::from(md);
                        let client_secret = String::from(client_secret);
                        self.add_secure(md, client_id, client_secret, None);
                    };

                    None
                });
        } // for yaml in yaml::Array
        self
    }
}
