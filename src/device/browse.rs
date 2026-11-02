// Copyright (c) 2026 Henrik Widlund
// SPDX-License-Identifier: MPL-2.0

//! Media browsing: the installed apps and the Up Next queue.

use crate::device::state::Snapshot;
use crate::errors::ServiceError;
use uc_api::intg::ws::BrowseMediaResponseMsgData;
use uc_api::model::{Pagination, Paging};
use uc_api::{BrowseMediaItem, MediaClass};

/// Media id prefix of an app. The rest is the bundle id.
pub const APP_PREFIX: &str = "app:";
/// Media id prefix of an Up Next item. The rest is the stable queue item id.
pub const QUEUE_PREFIX: &str = "queue:";

const ROOT: &str = "root";
const APPS: &str = "apps";
const UP_NEXT: &str = "up_next";

/// Browse one level.
pub fn browse(
    s: &Snapshot,
    media_id: Option<&str>,
    paging: &Paging,
) -> Result<BrowseMediaResponseMsgData, ServiceError> {
    let offset = paging.offset() as usize;
    let limit = paging.limit() as usize;
    let (mut root, count, items) = match media_id.unwrap_or(ROOT) {
        ROOT | "" => {
            let items = [
                directory(APPS, "Apps", MediaClass::App),
                directory(UP_NEXT, "Up Next", MediaClass::Playlist),
            ];
            let count = items.len();
            (
                directory(ROOT, "Apple TV", MediaClass::Directory),
                count,
                items.into_iter().skip(offset).take(limit).collect(),
            )
        }
        APPS => {
            let items = s
                .apps
                .iter()
                .skip(offset)
                .take(limit)
                .map(|a| BrowseMediaItem {
                    can_play: Some(true),
                    ..item(
                        format!("{APP_PREFIX}{}", a.bundle_id),
                        a.name.clone(),
                        MediaClass::App,
                    )
                })
                .collect();
            (
                directory(APPS, "Apps", MediaClass::App),
                s.apps.len(),
                items,
            )
        }
        UP_NEXT => {
            let playable = s.up_next.iter().filter_map(|q| Some((q.id.as_deref()?, q)));
            let count = playable.clone().count();
            let items = playable
                .skip(offset)
                .take(limit)
                .map(|(id, q)| BrowseMediaItem {
                    artist: q.artist.clone().filter(|a| !a.is_empty()),
                    duration: q.duration.map(|d| d.max(0.0).round() as u32),
                    can_play: Some(true),
                    ..item(
                        format!("{QUEUE_PREFIX}{id}"),
                        q.title
                            .clone()
                            .filter(|t| !t.is_empty())
                            .unwrap_or_else(|| "Unknown".into()),
                        MediaClass::Track,
                    )
                })
                .collect();
            (
                directory(UP_NEXT, "Up Next", MediaClass::Playlist),
                count,
                items,
            )
        }
        other => return Err(ServiceError::NotFound(format!("Unknown media_id: {other}"))),
    };
    root.items = Some(items);
    Ok(BrowseMediaResponseMsgData {
        media: Some(root),
        pagination: Pagination::new(count as u32, paging.limit(), paging.page()),
    })
}

fn directory(id: &str, title: &str, class: MediaClass) -> BrowseMediaItem {
    BrowseMediaItem {
        can_browse: Some(true),
        ..item(id.into(), title.into(), class)
    }
}

const fn item(media_id: String, title: String, class: MediaClass) -> BrowseMediaItem {
    BrowseMediaItem {
        media_id,
        title,
        subtitle: None,
        artist: None,
        album: None,
        media_class: Some(class),
        media_type: None,
        can_browse: None,
        can_play: None,
        can_search: None,
        thumbnail: None,
        duration: None,
        items: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::state::AppInfo;
    use ratv::mrp::QueueItem;

    #[test]
    fn browse_root_and_apps() {
        let s = Snapshot {
            apps: (0..15)
                .map(|i| AppInfo {
                    bundle_id: format!("id{i}"),
                    name: format!("App {i}"),
                })
                .collect(),
            ..Snapshot::default()
        };
        let root = browse(&s, None, &Paging::default()).expect("ok");
        assert_eq!(2, root.media.expect("media").items.expect("items").len());

        let page = browse(&s, Some(APPS), &Paging::new(2, 10)).expect("ok");
        let items = page.media.expect("media").items.expect("items");
        assert_eq!(5, items.len());
        assert_eq!("app:id10", items[0].media_id);
        assert_eq!(Some(15), page.pagination.count);
        assert!(browse(&s, Some("x"), &Paging::default()).is_err());
    }

    #[test]
    fn up_next_needs_ids() {
        let mut a = QueueItem::default();
        a.id = Some("1".into());
        a.title = Some("Song".into());
        let s = Snapshot {
            up_next: vec![QueueItem::default(), a],
            ..Snapshot::default()
        };
        let page = browse(&s, Some(UP_NEXT), &Paging::default()).expect("ok");
        let items = page.media.expect("media").items.expect("items");
        assert_eq!(1, items.len());
        assert_eq!("queue:1", items[0].media_id);
    }
}
