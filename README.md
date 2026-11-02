# Apple TV integration for Remote Two/3 (Rust)

Integration driver for the [Unfolded Circle](https://www.unfoldedcircle.com/) Remote Two and Remote 3.
It controls Apple TV devices on the local network with the [ratv](https://github.com/henrikwidlund/ratv) library.

The driver id is `appletv_rs`, so it can run next to the Apple TV integration that comes with the Remote.

## Features

- **Media player**: power, playback, seek, fast forward and rewind, volume and mute, repeat and shuffle,
  audio track and subtitles, navigation, app list, audio outputs, browse (apps and Up Next).
  Title, artist, album, artwork, position and media type.
- **Remote**: buttons, hold, repeat and button sequences.
- **Selects**: app, audio output, audio language, subtitles, account, chapter, Up Next, playback speed,
  shuffle and repeat.
- **Sensors**: the current value of each select, episode, genre, content rating, audio format,
  live content and volume control.
- **Voice assistant**: Siri with the voice button of the Remote.

One pairing with one PIN covers all features.

## Requirements

- Apple TV HD (4th generation) or Apple TV 4K (all generations). Older models are not shown in the setup.
  Tested with an Apple TV 4K (1st generation) on tvOS 26.
- The Apple TV and the Remote are on the same network or use a mDNS reflector. Multicast DNS (mDNS) must not be blocked.
- On the Apple TV, in the AirPlay settings: set _Allow Access_ to _Anyone on the Same Network_.
- A fixed IP address for the Apple TV (DHCP reservation) is recommended. The driver then finds the Apple TV
  faster after a restart.

## Known limits

- Volume control works only if the Apple TV can change the volume of the TV or receiver (HDMI CEC).
  Otherwise the volume features are removed.
- The Apple TV does not report the app in front. The app select shows the app opened through the driver or
  the app that plays media.
- The Apple TV does not report the current account. The account select shows the last account chosen
  through the driver.
- Metadata, artwork and playback speeds depend on the app. Many apps send only some of them.

## Install on the Remote

1. Download `uc-intg-appletv-rs-<version>-aarch64.tar.gz` from the [releases](https://github.com/henrikwidlund/uc-intg-appletv/releases).
2. Web configurator: Integrations > Add new > Install custom. Choose the file.
3. Start the setup of _Apple TV (Rust)_. Leave the IP address empty to search the network.
4. Choose the Apple TV and enter the PIN that the TV shows.

Start the setup again to add, remove, configure or pair a device again.
If the Apple TV is not found after a tvOS update, use _Configure selected device_ and choose it in
_Apple TV on the network_. The entity ids stay the same.

## Run on a computer

The driver can also run on a computer as an external integration. The Remote finds it with mDNS.

```shell
cargo run --release
```

| Variable                   | Default             | Description                                                            |
|----------------------------|---------------------|------------------------------------------------------------------------|
| `UC_CONFIG_HOME`           | working directory   | Folder for the configuration, cache and artwork files.                 |
| `UC_INTEGRATION_INTERFACE` | `0.0.0.0`           | Network interface to listen on.                                        |
| `UC_INTEGRATION_HTTP_PORT` | `9090`              | WebSocket and artwork port.                                            |
| `UC_DISABLE_MDNS_PUBLISH`  | `false`             | Do not announce the driver with mDNS.                                  |
| `UC_SETUP_TIMEOUT`         | `300`               | Setup timeout in seconds. `0` disables it.                             |
| `UC_CLIENT_NAME`           | host name           | Name shown on the Apple TV for the pairing.                            |
| `UC_API_MSG_TRACING`       | off                 | Log WebSocket messages: `all`, `in` or `out`. Can contain PINs.        |
| `RUST_LOG`                 | `info`              | Log level, for example `info,uc_intg_appletv=debug`.                   |

`config.json` contains the pairing keys. Only the owner can read it.

## Build

```shell
cargo build --release
```

The release build for the Remote uses the [UCR2 toolchain](https://github.com/unfoldedcircle/ucr2-toolchain).
See `.github/workflows/build.yml`. A `v*` tag creates a GitHub release with the archive.

### Development

```shell
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo deny check
```

`tools/manual_test.sh` runs the tests with a real Apple TV and Remote, one step at a time.
It saves the results to `manual-test-results-<time>.md`.

ratv is a git dependency with a fixed commit. To test local ratv changes, add this to
`~/.cargo/config.toml` and remove it when you are done:

```toml
[patch."https://github.com/henrikwidlund/ratv"]
ratv = { path = "/path/to/ratv/crates/ratv" }
```

## License

[MPL-2.0](LICENSE). Parts of the code come from the
[Home Assistant integration](https://github.com/unfoldedcircle/integration-home-assistant) by Unfolded Circle,
which uses the same license.
