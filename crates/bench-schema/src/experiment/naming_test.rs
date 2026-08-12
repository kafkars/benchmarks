//! Tests pinning the character set a name has to survive to become a topic.

use crate::is_topic_charset_safe;

#[test]
fn the_topic_charset_matches_what_kafka_accepts() {
    assert!(is_topic_charset_safe("kfb-0123456789abcdef-kafkars"));
    assert!(is_topic_charset_safe("a.b_c-1"));
    assert!(!is_topic_charset_safe("."));
    assert!(!is_topic_charset_safe(".."));
    assert!(!is_topic_charset_safe(""));
    assert!(!is_topic_charset_safe("a b"));
    assert!(!is_topic_charset_safe("a/b"));
}
