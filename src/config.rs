use config as config_external_crate;
use config_external_crate::{Config, File, FileFormat};
use crossterm::event::KeyCode;
use directories::ProjectDirs;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
pub enum Action {
    Up,
    Down,
    Left,
    Right,
    PageUp,
    PageDown,
    Quit,
    Reset,
    NextTab,
    PrevTab,
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub keymap: Keymap,
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    keybindings: HashMap<String, Action>,
}

#[derive(Debug, Clone)]
pub struct Keymap {
    bindings: HashMap<KeyCode, Action>,
    reverse: HashMap<Action, KeyCode>,
}

impl Keymap {
    pub fn get_action(&self, code: KeyCode) -> Option<Action> {
        self.bindings.get(&code).copied()
    }

    pub fn get_keycode(&self, action: Action) -> Option<KeyCode> {
        self.reverse.get(&action).copied()
    }
}

fn default_keybindings() -> Keymap {
    let mut bindings = HashMap::new();

    bindings.insert(KeyCode::Char('k'), Action::Up);
    bindings.insert(KeyCode::Char('j'), Action::Down);
    bindings.insert(KeyCode::Char('h'), Action::Left);
    bindings.insert(KeyCode::Char('l'), Action::Right);
    bindings.insert(KeyCode::Char('u'), Action::PageUp);
    bindings.insert(KeyCode::Char('d'), Action::PageDown);
    bindings.insert(KeyCode::Char('q'), Action::Quit);
    bindings.insert(KeyCode::Esc, Action::Reset);
    bindings.insert(KeyCode::Tab, Action::NextTab);
    bindings.insert(KeyCode::BackTab, Action::PrevTab);

    let reverse = bindings
        .iter()
        .map(|(k, v)| (*v, *k))
        .collect::<HashMap<_, _>>();

    Keymap { bindings, reverse }
}

fn parse_keycode(s: &str) -> Result<KeyCode, String> {
    Ok(match s {
        // single characters
        c if c.len() == 1 => KeyCode::Char(c.chars().next().unwrap()),

        "Backspace" => KeyCode::Backspace,
        "Enter" => KeyCode::Enter,
        "Left" => KeyCode::Left,
        "Right" => KeyCode::Right,
        "Up" => KeyCode::Up,
        "Down" => KeyCode::Down,
        "Home" => KeyCode::Home,
        "End" => KeyCode::End,
        "PageUp" => KeyCode::PageUp,
        "PageDown" => KeyCode::PageDown,
        "Tab" => KeyCode::Tab,
        "BackTab" => KeyCode::BackTab,
        "Delete" => KeyCode::Delete,
        "Insert" => KeyCode::Insert,
        "Esc" | "Escape" => KeyCode::Esc,

        // Function keys
        k if k.starts_with("F") => {
            let n = k[1..]
                .parse::<u8>()
                .map_err(|_| format!("Bad function key: {}", k))?;
            KeyCode::F(n)
        }

        _ => return Err(format!("Unknown key '{}'", s)),
    })
}

fn build_keymap(raw: RawConfig) -> Keymap {
    let mut bindings = default_keybindings().bindings;

    for (key_str, action) in raw.keybindings {
        match parse_keycode(&key_str) {
            Ok(code) => {
                // Remove any existing key that maps to the same action
                bindings.retain(|_, &mut a| a != action);

                // Insert the new key
                bindings.insert(code, action);
            }
            Err(err) => eprintln!("[config] bad key '{}': {} — ignored", key_str, err),
        }
    }

    let reverse = bindings
        .iter()
        .map(|(k, v)| (*v, *k))
        .collect::<HashMap<_, _>>();

    Keymap { bindings, reverse }
}

pub fn load_config() -> AppConfig {
    let proj_dirs = ProjectDirs::from("", "", "parqeye").unwrap();
    let config_dir = proj_dirs.config_dir();
    let path = config_dir.join("config.toml");
    let path_str = path.to_string_lossy();

    // Return defaults if config file does not exist
    if !path.exists() {
        return AppConfig {
            keymap: default_keybindings(),
        };
    }

    // Load and parse the config file
    let raw = match Config::builder()
        .add_source(File::new(path_str.as_ref(), FileFormat::Toml).required(false))
        .build()
    {
        Ok(cfg) => match cfg.try_deserialize::<RawConfig>() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[config] Invalid TOML: {} — using defaults", e);
                return AppConfig {
                    keymap: default_keybindings(),
                };
            }
        },
        Err(e) => {
            eprintln!("[config] Cannot read config: {} — using defaults", e);
            return AppConfig {
                keymap: default_keybindings(),
            };
        }
    };

    AppConfig {
        keymap: build_keymap(raw),
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            keymap: default_keybindings(),
        }
    }
}
