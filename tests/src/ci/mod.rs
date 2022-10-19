#[cfg(test)]
mod context {
    pub(super) struct Context {
        pub(super) redis_server: &'static str,
        pub(super) mc_server: &'static str,
    }

    static CTX: Context = Context {
        redis_server: env!("BRZ_CI_REDIS_SERVER"),
        mc_server: env!("BRZ_CI_MC_SERVER"),
    };

    pub(super) fn get() -> &'static Context {
        &CTX
    }
}
