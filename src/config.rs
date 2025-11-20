use config as config_external_crate;
use config_external_crate::{Config, File, FileFormat};
use crossterm::event::KeyCode;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum Action {
    Up,
    Down,
    Left,
    Right,
    PageUp,
    PageDown,
    Quit,
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
}

impl Keymap {
    pub fn get_action(&self, code: KeyCode) -> Option<Action> {
        self.bindings.get(&code).copied()
    }
}

fn default_keybindings() -> Keymap {
    let mut map = HashMap::new();

    map.insert(KeyCode::Up, Action::Up);
    map.insert(KeyCode::Down, Action::Down);
    map.insert(KeyCode::Left, Action::Left);
    map.insert(KeyCode::Right, Action::Right);
    map.insert(KeyCode::PageUp, Action::PageUp);
    map.insert(KeyCode::PageDown, Action::PageDown);
    map.insert(KeyCode::Esc, Action::Quit);

    Keymap { bindings: map }
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
    let mut map = default_keybindings().bindings;

    for (key_str, action) in raw.keybindings {
        match parse_keycode(&key_str) {
            Ok(code) => {
                // Remove any existing key that maps to the same action
                map.retain(|_, &mut a| a != action);

                // Insert the new key
                map.insert(code, action);
            }
            Err(err) => eprintln!("[config] bad key '{}': {} — ignored", key_str, err),
        }
    }

    Keymap { bindings: map }
}

pub fn load_config() -> AppConfig {
    let path = format!(
        "{}/.config/parqeye/config.toml",
        std::env::var("HOME").unwrap()
    );

    // Try loading config file
    let raw = match Config::builder()
        .add_source(File::new(&path, FileFormat::Toml).required(false))
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
