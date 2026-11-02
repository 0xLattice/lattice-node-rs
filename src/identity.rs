//! Process identity is loaded once at startup; the pinned Swift runtime does not
//! rotate its signing key in place. Unix checks inspect the descriptor read.
use anyhow::{Context, ensure};
use ed25519_dalek::SigningKey;
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::Path,
};
const MAX_KEY_FILE: u64 = 16 * 1024;

pub(crate) fn load_or_create(path: &Path) -> anyhow::Result<SigningKey> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // NONBLOCK ensures a FIFO cannot hang startup before fstat rejects it.
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let key = SigningKey::generate(&mut rand_core::OsRng);
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
            }
            // Never replace an existing identity, including a competing creator.
            let mut file = options.open(path)?;
            file.write_all(hex::encode(key.to_bytes()).as_bytes())?;
            file.sync_all()?;
            return Ok(key);
        }
        Err(error) => return Err(error).context("opening process identity"),
    };
    let metadata = file.metadata()?;
    ensure!(metadata.is_file(), "identity must be a regular file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "identity permissions must not grant group or other access"
        );
    }
    // Windows ACL validation is not implemented. The regular-file and byte
    // bounds below apply there, but are not a claim of private ACL enforcement.
    ensure!(
        metadata.len() <= MAX_KEY_FILE,
        "identity exceeds size limit"
    );
    let mut bytes = Vec::new();
    file.take(MAX_KEY_FILE + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_KEY_FILE,
        "identity exceeds size limit"
    );
    let text = std::str::from_utf8(&bytes)
        .context("identity file is not UTF-8")?
        .trim();
    let hex_key = if text.starts_with('{') {
        let object: serde_json::Value =
            serde_json::from_str(text).context("invalid identity JSON")?;
        object
            .get("privateKey")
            .and_then(serde_json::Value::as_str)
            .context("identity JSON requires privateKey")?
            .to_owned()
    } else {
        text.to_owned()
    };
    let key: [u8; 32] = hex::decode(hex_key)
        .map_err(|_| anyhow::anyhow!("identity must contain 32-byte private-key hex"))?
        .try_into()
        .map_err(|_| anyhow::anyhow!("identity must contain 32-byte private-key hex"))?;
    Ok(SigningKey::from_bytes(&key))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generated_identity_persists_and_raw_json_formats_keep_the_same_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity/key");
        let created = load_or_create(&path).unwrap();
        assert_eq!(
            load_or_create(&path).unwrap().to_bytes(),
            created.to_bytes()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        std::fs::write(
            &path,
            serde_json::json!({"privateKey":hex::encode(created.to_bytes())}).to_string(),
        )
        .unwrap();
        assert_eq!(
            load_or_create(&path).unwrap().verifying_key(),
            created.verifying_key()
        );
        std::fs::write(
            &path,
            format!(" {}\n", hex::encode_upper(created.to_bytes())),
        )
        .unwrap();
        assert_eq!(
            load_or_create(&path).unwrap().verifying_key(),
            created.verifying_key()
        );
        for bad in ["{\"publicKey\":\"only\"}", "not-hex", "00"] {
            std::fs::write(&path, bad).unwrap();
            assert!(load_or_create(&path).is_err());
            assert_eq!(std::fs::read_to_string(&path).unwrap(), bad);
        }
        std::fs::write(&path, vec![b'0'; MAX_KEY_FILE as usize + 1]).unwrap();
        assert!(
            load_or_create(&path)
                .unwrap_err()
                .to_string()
                .contains("size limit")
        );
    }
    #[cfg(unix)]
    #[test]
    fn private_descriptor_rejects_readable_keys_links_directories_and_devices() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        load_or_create(&path).unwrap();
        for mode in [0o640, 0o604, 0o644, 0o660] {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            assert!(
                load_or_create(&path)
                    .unwrap_err()
                    .to_string()
                    .contains("permissions")
            );
        }
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join("link");
        symlink(&path, &link).unwrap();
        assert!(load_or_create(&link).is_err());
        let missing = dir.path().join("missing");
        let dangling = dir.path().join("dangling");
        symlink(&missing, &dangling).unwrap();
        assert!(load_or_create(&dangling).is_err());
        assert!(!missing.exists());
        assert!(load_or_create(dir.path()).is_err());
        assert!(load_or_create(Path::new("/dev/null")).is_err());
    }
}
