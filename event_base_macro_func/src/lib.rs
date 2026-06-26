pub mod send_msg;
pub mod start_system;

#[macro_export]
macro_rules! send_msg {
    ($msg:expr, $try_send:expr, $time_out:expr) => {{
        use event_base_macro_func::send_msg::send_msg_impl;
        send_msg_impl($msg, $try_send, $time_out).await
    }};
}

#[macro_export]
macro_rules! start_system {
    ($producer:expr, $wal:expr, $system_builder:expr, $node_type: expr) => {{
        use event_base_macro_func::start_system::start_system_impl;
        start_system_impl($node_type, $producer, $wal, $system_builder).await
    }};
}
