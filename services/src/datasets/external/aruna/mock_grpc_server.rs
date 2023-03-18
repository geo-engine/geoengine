use crate::error::{Error, Result};
use geoengine_operators::util::retry::retry;
use prost::Message;
use rand::Rng;
use std::collections::HashMap;
use std::convert::Infallible;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use tonic::codec::ProstCodec;
use tonic::codegen::{http, Body, BoxFuture};
use tonic::transport::server::Router;
use tonic::Code;

pub type InfallibleHttpResponseFuture =
    tonic::codegen::BoxFuture<http::Response<tonic::body::BoxBody>, Infallible>;

pub(super) struct MockGRPCServer {
    address: SocketAddr,
    _thread: Arc<Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>>,
}

impl MockGRPCServer {
    pub async fn start_with_router(builder: Router) -> Result<Self, Error> {
        let port = Self::find_open_port(1_000).ok_or(Error::ServerStartup)?;
        let address: SocketAddr = format!("[::1]:{port}").parse().unwrap();

        let thread = tokio::spawn(builder.serve(address));

        retry(10, 10, 2.0, None, || async move {
            TcpStream::connect_timeout(&address, std::time::Duration::from_millis(25))
        })
        .await
        .map_err(|_err| Error::ServerStartup)?;

        let server = MockGRPCServer {
            address,
            _thread: Arc::new(Some(thread)),
        };
        Ok(server)
    }

    fn find_open_port(num_tries: u16) -> Option<u16> {
        for _ in 0..num_tries {
            let mut rng = rand::thread_rng();
            let port: u16 = rng.gen_range(50000..60000);
            let address: SocketAddr = format!("[::1]:{port}").parse().unwrap();
            if TcpListener::bind(address).is_ok() {
                return Some(port);
            }
        }
        None
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }
}

#[derive(Clone)]
pub struct MapResponseService<Key, Request, Response, F> {
    pub item: HashMap<Key, Response>,
    pub key_func: F,
    _phantom: PhantomData<Request>,
}

impl<Request, Key, Res, F> tonic::server::UnaryService<Request>
    for MapResponseService<Key, Request, Res, F>
where
    Key: Clone + Eq + Hash + Send,
    Request: Clone + Send + 'static,
    Res: Clone + Send + 'static,
    F: FnMut(Request) -> Key + Clone + Send,
{
    type Response = Res;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<Request>) -> Self::Future {
        let key = (self.key_func)(request.into_inner());
        let response = self.item.get(&key);
        match response {
            None => {
                let status = tonic::Status::new(Code::NotFound, "Unknown request key");
                let fut = async move { Err(status) };
                Box::pin(fut)
            }
            Some(value) => {
                let owned_value = value.to_owned();
                let fut = async move { Ok(tonic::Response::new(owned_value)) };
                Box::pin(fut)
            }
        }
    }
}

impl<
        Key: Clone + Eq + Hash + Send + 'static,
        RequestMessage: Message + Default + Clone + 'static,
        ResponseMessage: Message + Clone + 'static,
        F: FnMut(RequestMessage) -> Key + Clone + Send + 'static,
    > MapResponseService<Key, RequestMessage, ResponseMessage, F>
{
    pub fn new(item: HashMap<Key, ResponseMessage>, key_func: F) -> Self {
        MapResponseService {
            item,
            key_func,
            _phantom: Default::default(),
        }
    }

    pub fn request_success<B>(self, req: http::Request<B>) -> InfallibleHttpResponseFuture
    where
        B: Body + Send + 'static,
        B::Error: Into<tonic::codegen::StdError> + Send + 'static,
    {
        Box::pin(async move {
            let codec: ProstCodec<ResponseMessage, RequestMessage> =
                tonic::codec::ProstCodec::default();
            let mut grpc = tonic::server::Grpc::new(codec);
            let res = grpc.unary(self, req).await;
            Ok(res)
        })
    }
}

#[macro_export]
macro_rules! generate_mapping_grpc_service {
    ($service_name:literal, $struct_name: ident, $($method_name: literal, $request: ty, $response: ty, $field_name: ident, $key_generic: ident, $key_type: tt, $key_func_name: ident, )+) => {

        #[derive(Clone)]
        struct $struct_name<$($key_generic, )+> {
            $($field_name: MapResponseService<$key_type, $request, $response, $key_generic>, )+
        }

        impl<B, $($key_generic, )+> Service<Request<B>> for $struct_name<$($key_generic, )+>
            where
                B: Body + Send + 'static,
                B::Error: Into<tonic::codegen::StdError> + Send + 'static,
                $($key_generic: FnMut($request) -> $key_type + Clone + Send + 'static, )+
        {
            type Response = http::Response<tonic::body::BoxBody>;
            type Error = Infallible;
            type Future = InfallibleHttpResponseFuture;

            fn poll_ready(
                &mut self,
                _cx: &mut std::task::Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, req: Request<B>) -> Self::Future {
                match req.uri().path() {
                    $($method_name => {
                        self.$field_name.clone().request_success::<B>(req)
                    },)+
                    _ => {
                        let builder = http::Response::builder()
                            .status(200)
                            .header("content-type", "application/grpc")
                            .header("grpc-status", format!("{}", Code::Unimplemented as u32));

                        Box::pin(async move {
                            let body = builder.body(tonic::body::empty_body()).unwrap();
                            Ok(body)
                        })
                    },
                }
            }
        }

        impl<$($key_generic, )+> tonic::transport::NamedService for $struct_name<$($key_generic, )+> {
            const NAME: &'static str = $service_name;
        }

    }
}
