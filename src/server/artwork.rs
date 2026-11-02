// Copyright (c) 2026 Henrik Widlund
// SPDX-License-Identifier: MPL-2.0

//! HTTP route for artwork that has no URL.
//!
//! The image from the Apple TV is written to a file. The last images of each device are kept,
//! so an image that comes again is not written again. The file name comes from the artwork id,
//! so the Remote can cache the image.

use crate::config_store::fnv1a;
use actix_web::http::header;
use actix_web::{HttpResponse, get, web};
use log::{debug, warn};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

/// Time the Remote may cache an image. A file name always has the same image.
const CACHE_CONTROL: &str = "max-age=86400";

/// Images kept per device.
const KEEP: usize = 16;

/// Image files per device, shared between the device actors and the HTTP route.
#[derive(Debug, Clone)]
pub struct ArtworkStore {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    dir: PathBuf,
    /// File names per device, the oldest first.
    files: Mutex<HashMap<Box<str>, VecDeque<Box<str>>>>,
}

impl ArtworkStore {
    /// Use `dir` for the image files. Files from an earlier run are removed.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        if let Err(e) = std::fs::remove_dir_all(&dir)
            && e.kind() != io::ErrorKind::NotFound
        {
            warn!("Could not remove old artwork in {}: {e}", dir.display());
        }
        Self {
            inner: Arc::new(Inner {
                dir,
                files: Mutex::default(),
            }),
        }
    }

    /// Write the image of a device, unless it is kept already. The oldest image is removed
    /// when more than [`KEEP`] are kept. Returns the file name.
    pub async fn save(
        &self,
        device: &str,
        artwork_id: &str,
        mime_type: Option<&str>,
        data: &[u8],
    ) -> io::Result<String> {
        let ext = extension(mime_type.unwrap_or("image/jpeg"));
        let name = format!("{:016x}.{ext}", fnv1a(artwork_id.as_bytes()));
        if self.touch(device, &name) {
            return Ok(name);
        }
        tokio::fs::create_dir_all(&self.inner.dir).await?;
        let path = self.path(device, &name);
        let tmp = path.with_extension("tmp");
        tokio::fs::write(&tmp, data).await?;
        tokio::fs::rename(&tmp, &path).await?;
        let mut files = self.lock();
        let list = files.entry(device.into()).or_default();
        if !list.iter().any(|n| **n == *name) {
            list.push_back(name.as_str().into());
            while list.len() > KEEP {
                if let Some(old) = list.pop_front() {
                    remove_file(self.path(device, &old));
                }
            }
        }
        Ok(name)
    }

    /// Mark the image as used now. False if it is not kept.
    pub fn touch(&self, device: &str, name: &str) -> bool {
        let mut files = self.lock();
        let Some(list) = files.get_mut(device) else {
            return false;
        };
        let Some(i) = list.iter().position(|n| **n == *name) else {
            return false;
        };
        if let Some(n) = list.remove(i) {
            list.push_back(n);
        }
        true
    }

    /// Remove all images of a device.
    pub fn remove(&self, device: &str) {
        let list = self.lock().remove(device);
        if let Some(list) = list {
            for name in list {
                remove_file(self.path(device, &name));
            }
        }
    }

    /// Path of the image if it is kept for the device.
    fn file(&self, device: &str, name: &str) -> Option<PathBuf> {
        let files = self.lock();
        files
            .get(device)
            .is_some_and(|l| l.iter().any(|n| **n == *name))
            .then(|| self.path(device, name))
    }

    fn path(&self, device: &str, name: &str) -> PathBuf {
        self.inner.dir.join(format!("{device}-{name}"))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Box<str>, VecDeque<Box<str>>>> {
        // The map stays valid if a holder panics.
        self.inner
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

fn remove_file(path: PathBuf) {
    if let Err(e) = std::fs::remove_file(&path)
        && e.kind() != io::ErrorKind::NotFound
    {
        warn!("Could not remove {}: {e}", path.display());
    }
}

fn extension(mime_type: &str) -> &'static str {
    match mime_type {
        "image/png" => "png",
        "image/heic" => "heic",
        "image/webp" => "webp",
        _ => "jpg",
    }
}

fn mime_type(name: &str) -> &'static str {
    match name.rsplit('.').next() {
        Some("png") => "image/png",
        Some("heic") => "image/heic",
        Some("webp") => "image/webp",
        _ => "image/jpeg",
    }
}

/// Serve a kept image of a device.
#[get("/artwork/{device}/{name}")]
pub async fn artwork_index(
    path: web::Path<(String, String)>,
    store: web::Data<ArtworkStore>,
) -> HttpResponse {
    let (device, name) = path.into_inner();
    // Only known names are served, so the path cannot leave the directory.
    let Some(file) = store.file(&device, &name) else {
        debug!("Artwork {device}/{name} is not kept");
        return HttpResponse::NotFound().finish();
    };
    tokio::fs::read(&file).await.map_or_else(
        |_| HttpResponse::NotFound().finish(),
        |data| {
            HttpResponse::Ok()
                .insert_header((header::CONTENT_TYPE, mime_type(&name)))
                .insert_header((header::CACHE_CONTROL, CACHE_CONTROL))
                .body(data)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_web::test]
    async fn last_images_are_kept() {
        let dir = std::env::temp_dir().join(format!("uc-intg-appletv-art-{}", std::process::id()));
        let store = ArtworkStore::new(&dir);
        let a = store
            .save("dev", "id-a", Some("image/png"), b"png")
            .await
            .expect("save");
        let path_a = store.file("dev", &a).expect("kept");
        assert!(store.file("dev", "../x").is_none());

        // The same artwork id gives the same name, so the Remote can use its cache.
        let again = store
            .save("dev", "id-a", Some("image/png"), b"other")
            .await
            .expect("save");
        assert_eq!(a, again);
        assert_eq!(b"png".as_slice(), std::fs::read(&path_a).expect("read"));

        let b = store.save("dev", "id-b", None, b"jpg").await.expect("save");
        assert_ne!(a, b);
        assert!(path_a.exists());
        assert_eq!("image/jpeg", mime_type(&b));

        // `a` is used again, so `b` is the oldest and is removed first.
        assert!(store.touch("dev", &a));
        for i in 0..KEEP - 1 {
            store
                .save("dev", &format!("id-{i}"), None, b"x")
                .await
                .expect("save");
        }
        assert!(store.file("dev", &b).is_none());
        assert!(store.file("dev", &a).is_some());

        store.remove("dev");
        assert!(store.file("dev", &a).is_none());
        assert!(!path_a.exists());
        let _ = std::fs::remove_dir_all(dir);
    }
}
