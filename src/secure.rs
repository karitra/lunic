use cocaine::service::Tvm;
use cocaine::{Error, Service};
use cocaine::service::tvm::Grant;

use futures;
use futures::future::{Future};

use time;

use config::{Config, Secure};
use std::rc::Rc;

pub trait SecureProxy {
    fn ticket_as_header(&mut self) -> Box<Future<Item = Option<String>, Error = Error>>;
}

struct PromiscuousProxy;

impl PromiscuousProxy {
    pub fn new() -> PromiscuousProxy {
        PromiscuousProxy{}
    }
}

struct TvmProxy {
    tvm: Tvm,
    last_updated: i64,
    ticket_expire_sec: Option<i64>,
    cached_ticket: Rc<Option<String>>,
    secure: Secure,
}

impl TvmProxy {
    pub fn new(service: Service, ticket_expire_sec: &Option<i64>, secure: &Secure) -> TvmProxy {
        TvmProxy{
            tvm: Tvm::new(service),
            last_updated: 0,
            ticket_expire_sec: ticket_expire_sec.clone(),
            cached_ticket: Rc::new(None),
            secure: secure.clone(),
        }
    }

    pub fn ticket(&mut self) -> Box<Future<Item = String, Error = Error>> {
        match self.ticket_expire_sec {
            Some(to_expire) =>
                if self.last_updated < time::get_time().sec - to_expire {
                    self.update_and_get_cached()
                } else {
                    self.get_or_fetch()
                },
            None => self.fetch_ticket()
        }
    }

    fn get_or_fetch(&self) -> Box<Future<Item = String, Error = Error>> {
        if let Some(ref token) = *self.cached_ticket {
            Box::new(futures::future::ok(token.clone()))
        } else {
            self.update_and_get_cached()
        }
    }

    fn update_and_get_cached(&self) -> Box<Future<Item = String, Error = Error>> {
        let mut cached_ticket = Rc::clone(&self.cached_ticket);
        let future = self.fetch_ticket().and_then(move |token| {
            cached_ticket = Rc::new(Some(token.clone()));
            Ok(token)
        });

        Box::new(future)
    }

    fn fetch_ticket(&self) -> Box<Future<Item = String, Error = Error>> {
        let grant: Grant = self.secure.grant.clone().unwrap_or(Grant::ClientCredentials);
        let future = self.tvm.ticket(self.secure.client_id as u32, &self.secure.client_secret, &grant);

        Box::new(future)
    }
}

impl SecureProxy for TvmProxy {
    fn ticket_as_header(&mut self) -> Box<Future<Item = Option<String>, Error = Error>> {
        let ty = self.secure.get_mod();
        let header = self.ticket().and_then(move |token| {
            Ok(Some(format!("{} {}", ty, token)))
        });
        Box::new(header)
    }
}

impl SecureProxy for PromiscuousProxy {
    fn ticket_as_header(&mut self) -> Box<Future<Item = Option<String>, Error = Error>> {
        Box::new(futures::future::ok(None))
    }
}


pub fn make_ticket_service(service: Service, config: &Config) -> Box<SecureProxy>
{
    match config.secure {
        Some(ref secure) => match &secure.md[..] {
            "TVM" => Box::new(TvmProxy::new(service, &config.ticket_expire_sec, secure)),
            _ => Box::new(PromiscuousProxy::new())
        },
        None => Box::new(PromiscuousProxy::new())
    }
}
