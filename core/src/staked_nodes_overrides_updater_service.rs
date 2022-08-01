use {
    serde::Deserialize,
    std::{
        collections::HashMap,
        error,
        net::IpAddr,
        path::Path,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{self, sleep, Builder, JoinHandle},
        time::{Duration, Instant},
    },
    url::Url,
};

const DEFAULT_STAKE_OVERRIDES_PERIOD: Duration = Duration::from_secs(60);

#[derive(Default, Deserialize, Clone)]
pub struct StakedNodesOverrides {
    auto_reload: bool,
    pub stake_map: HashMap<IpAddr, u64>,
}

pub struct StakedNodesOverridesUpdaterService {
    thread_hdl: JoinHandle<()>,
}

impl StakedNodesOverridesUpdaterService {
    pub fn new(
        exit: Arc<AtomicBool>,
        shared_staked_nodes_override: Arc<RwLock<StakedNodesOverrides>>,
        path: Option<String>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("sol-wl-updater".to_string())
            .spawn(move || {
                if let Some(path) = path {
                    let mut last_reload_attempt = None;
                    let mut auto_reload = true;
                    while !exit.load(Ordering::Relaxed) && auto_reload {
                        let mut new_stake_map = HashMap::new();
                        if Self::try_reload(
                            &path,
                            &mut new_stake_map,
                            &mut auto_reload,
                            &mut last_reload_attempt,
                        ) {
                            let mut shared = shared_staked_nodes_override.write().unwrap();
                            shared.auto_reload = auto_reload;
                            shared.stake_map = new_stake_map;
                            debug!("Config for staked nodes weights has been loaded.");
                        }

                        sleep(Duration::from_millis(1));
                    }
                }
            })
            .unwrap();

        Self { thread_hdl }
    }

    fn try_config_from_url(url: Url) -> Result<StakedNodesOverrides, Box<dyn error::Error>> {
        let res = reqwest::blocking::get(url)?;
        let body = res.text()?;

        Ok(serde_yaml::from_str(&body)?)
    }

    fn try_config_from_local_file(
        path: &String,
    ) -> Result<StakedNodesOverrides, Box<dyn error::Error>> {
        let file = std::fs::File::open(path)?;

        Ok(serde_yaml::from_reader(file)?)
    }

    fn try_to_read_config(
        path_or_url: &String,
    ) -> Result<StakedNodesOverrides, Box<dyn error::Error>> {
        if Path::new(path_or_url).exists() {
            return Self::try_config_from_local_file(path_or_url);
        }
        if let Ok(url) = Url::parse(path_or_url) {
            return Self::try_config_from_url(url);
        }

        Err("Config cannot be loaded: No suitable reader.".into())
    }

    fn try_reload(
        path_or_url: &String,
        stake_map: &mut HashMap<IpAddr, u64>,
        auto_reload: &mut bool,
        last_reload_attempt: &mut Option<Instant>,
    ) -> bool {
        if let Some(last_reload_attempt) = last_reload_attempt {
            if last_reload_attempt.elapsed() < DEFAULT_STAKE_OVERRIDES_PERIOD {
                return false;
            }
        }

        *last_reload_attempt = Some(Instant::now());

        let staked_nodes_override = match Self::try_to_read_config(path_or_url) {
            Ok(config) => config,
            Err(err) => {
                error!("Error loading config for staked nodes weights: {}", err);
                return false;
            }
        };

        *stake_map = staked_nodes_override.stake_map;
        *auto_reload = staked_nodes_override.auto_reload;

        true
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
