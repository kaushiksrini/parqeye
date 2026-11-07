use base64::{Engine, engine::general_purpose::STANDARD};
use std::io::{self, Write};

// Cap the encoded payload, since terminal limits apply after base64 expansion.
const MAX_ENCODED_BYTES: usize = 100_000;

pub fn copy(value: &str, writer: &mut impl Write) -> io::Result<()> {
    if value.len() > MAX_ENCODED_BYTES / 4 * 3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cell too large to copy (OSC 52 limit: 100 KB)",
        ));
    }

    write!(writer, "\x1b]52;c;{}\x07", STANDARD.encode(value))?;
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_osc52_with_utf8_and_control_characters_encoded() {
        let value = "hello\n資料\x1b\x07";
        let mut output = Vec::new();
        copy(value, &mut output).unwrap();
        assert!(output.starts_with(b"\x1b]52;c;"));
        assert!(output.ends_with(b"\x07"));
        assert_eq!(
            STANDARD.decode(&output[7..output.len() - 1]).unwrap(),
            value.as_bytes()
        );

        output.clear();
        copy("", &mut output).unwrap();
        assert_eq!(output, b"\x1b]52;c;\x07");
    }

    #[test]
    fn rejects_oversized_payloads_before_writing_anything() {
        let mut output = Vec::new();
        let limit = MAX_ENCODED_BYTES / 4 * 3;
        copy(&"a".repeat(limit), &mut output).unwrap();
        assert_eq!(output.len(), MAX_ENCODED_BYTES + 8);

        output.clear();
        let error = copy(&"a".repeat(limit + 1), &mut output).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(output.is_empty());
    }
}
