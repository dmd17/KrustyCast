//! The few functions of Aeron's C client (aeronc.h, 1.54.0) the rig uses.
#![allow(non_camel_case_types, dead_code)]
use std::ffi::{c_char, c_int, c_void};

#[repr(C)]
pub struct aeron_context_t {
    _p: [u8; 0],
}
#[repr(C)]
pub struct aeron_t {
    _p: [u8; 0],
}
#[repr(C)]
pub struct aeron_exclusive_publication_t {
    _p: [u8; 0],
}
#[repr(C)]
pub struct aeron_async_add_exclusive_publication_t {
    _p: [u8; 0],
}
#[repr(C)]
pub struct aeron_subscription_t {
    _p: [u8; 0],
}
#[repr(C)]
pub struct aeron_async_add_subscription_t {
    _p: [u8; 0],
}
#[repr(C)]
pub struct aeron_header_t {
    _p: [u8; 0],
}
#[repr(C)]
pub struct aeron_image_t {
    _p: [u8; 0],
}

pub type aeron_fragment_handler_t =
    Option<unsafe extern "C" fn(*mut c_void, *const u8, usize, *mut aeron_header_t)>;
pub type aeron_on_image_t =
    Option<unsafe extern "C" fn(*mut c_void, *mut aeron_subscription_t, *mut aeron_image_t)>;
pub type aeron_reserved_value_supplier_t =
    Option<unsafe extern "C" fn(*mut c_void, *mut u8, usize) -> i64>;

unsafe extern "C" {
    pub fn aeron_context_init(context: *mut *mut aeron_context_t) -> c_int;
    pub fn aeron_context_set_dir(context: *mut aeron_context_t, value: *const c_char) -> c_int;
    pub fn aeron_context_close(context: *mut aeron_context_t) -> c_int;
    pub fn aeron_init(client: *mut *mut aeron_t, context: *mut aeron_context_t) -> c_int;
    pub fn aeron_start(client: *mut aeron_t) -> c_int;
    pub fn aeron_close(client: *mut aeron_t) -> c_int;
    pub fn aeron_async_add_exclusive_publication(
        async_: *mut *mut aeron_async_add_exclusive_publication_t,
        client: *mut aeron_t,
        uri: *const c_char,
        stream_id: i32,
    ) -> c_int;
    pub fn aeron_async_add_exclusive_publication_poll(
        publication: *mut *mut aeron_exclusive_publication_t,
        async_: *mut aeron_async_add_exclusive_publication_t,
    ) -> c_int;
    pub fn aeron_async_add_subscription(
        async_: *mut *mut aeron_async_add_subscription_t,
        client: *mut aeron_t,
        uri: *const c_char,
        stream_id: i32,
        on_available: aeron_on_image_t,
        on_available_clientd: *mut c_void,
        on_unavailable: aeron_on_image_t,
        on_unavailable_clientd: *mut c_void,
    ) -> c_int;
    pub fn aeron_async_add_subscription_poll(
        subscription: *mut *mut aeron_subscription_t,
        async_: *mut aeron_async_add_subscription_t,
    ) -> c_int;
    pub fn aeron_exclusive_publication_offer(
        publication: *mut aeron_exclusive_publication_t,
        buffer: *const u8,
        length: usize,
        reserved_value_supplier: aeron_reserved_value_supplier_t,
        clientd: *mut c_void,
    ) -> i64;
    pub fn aeron_exclusive_publication_is_connected(
        publication: *mut aeron_exclusive_publication_t,
    ) -> bool;
    pub fn aeron_subscription_poll(
        subscription: *mut aeron_subscription_t,
        handler: aeron_fragment_handler_t,
        clientd: *mut c_void,
        fragment_limit: usize,
    ) -> c_int;
    pub fn aeron_subscription_is_connected(subscription: *mut aeron_subscription_t) -> bool;
    pub fn aeron_errmsg() -> *const c_char;
}

pub fn errmsg() -> String {
    // SAFETY: aeron_errmsg returns a thread-local NUL-terminated string.
    unsafe { std::ffi::CStr::from_ptr(aeron_errmsg()) }
        .to_string_lossy()
        .into_owned()
}
