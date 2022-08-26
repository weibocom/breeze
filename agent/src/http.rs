cfg_if::cfg_if! {
if #[cfg(feature = "restful_api_enable")] {

// 必须运行在tokio的runtime环境中
pub(super) fn start_http_server(ctx: &context::Context, rt: &tokio::runtime::Runtime) {
    set_env_props(ctx);
    rt.spawn(api::start_whitelist_refresh(ctx.whitelist_host.clone()));
    log::info!("launch with rocket!");
    rt.spawn(async {
        if let Err(e) = api::routes().launch().await {
            log::error!("launch rocket failed: {:?}", e);
        };
    });
}
// 设置需要的变量到evns中
fn set_env_props(ctx: &context::Context) {
    let sp_name = ctx.service_path();
    let path = std::path::Path::new(&sp_name);
    let base_path = path.parent().unwrap();
    api::props::set_prop("base_path", base_path.to_str().unwrap());

    // 设置version
    api::props::set_prop("version", context::get_short_version());
}


}}
