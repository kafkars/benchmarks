//! The request-economics table, in both renderings.
//!
//! This is the one section whose contents come from outside the sealed summary:
//! `kafkars.suite-summary.v1` has no field for request economics, so they
//! travel beside it and are rendered from a separate list. Both renderings live
//! here together because the rule they share is the one that matters — a
//! subject whose client emits no native statistics is *absent* from the table
//! rather than present with a row of zeroes, and an empty table says so in a
//! sentence instead of printing a header over nothing.
//!
//! The Markdown half is also the bundle view's, which is why it is reachable
//! from [`super::bundle`] as well as from [`super::markdown`].

use std::fmt::Write as _;

use crate::suite::SubjectEconomics;

use super::format::{escape, optional_count, optional_rate, optional_ratio};

/// The request economics section, or the sentence that explains its absence.
pub(super) fn write_markdown_economics(out: &mut String, economics: &[SubjectEconomics]) {
    let _ = writeln!(out, "\n## Request economics\n");
    if economics.is_empty() {
        let _ = writeln!(
            out,
            "No subject reported native client statistics, so there is nothing to report here. \
             An absent measurement is not a measurement of zero.\n"
        );
        return;
    }
    let _ = writeln!(
        out,
        "What each client spent in broker traffic for the records it delivered. Only subjects \
         whose client emits native statistics appear.\n"
    );
    let _ = writeln!(
        out,
        "| Subject | Produce requests | Per million acknowledged | Records per request | \
         Payload share of wire bytes | Records per batch | Retries | Timeouts |"
    );
    let _ = writeln!(
        out,
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );
    for entry in economics {
        let totals = &entry.totals;
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            entry.subject,
            optional_count(totals.produce_requests),
            optional_rate(totals.produce_requests_per_million_acknowledged),
            optional_ratio(totals.records_per_produce_request),
            optional_ratio(totals.payload_bytes_per_transmitted_byte),
            optional_ratio(totals.batch_records.and_then(|window| window.mean())),
            optional_count(totals.retries),
            optional_count(totals.timeouts)
        );
    }
}

/// The request economics table, or the sentence that explains its absence.
pub(super) fn html_economics(out: &mut String, economics: &[SubjectEconomics]) {
    let _ = writeln!(out, "<h2>Request economics</h2>");
    if economics.is_empty() {
        let _ = writeln!(
            out,
            "<p>No subject reported native client statistics, so there is nothing to report \
             here. An absent measurement is not a measurement of zero.</p>"
        );
        return;
    }
    let _ = writeln!(
        out,
        "<table><thead><tr><th>Subject</th><th class=\"n\">Produce requests</th>\
         <th class=\"n\">Per million acknowledged</th><th class=\"n\">Records per request</th>\
         <th class=\"n\">Payload share of wire bytes</th><th class=\"n\">Records per batch</th>\
         <th class=\"n\">Retries</th><th class=\"n\">Timeouts</th></tr></thead><tbody>"
    );
    for entry in economics {
        let totals = &entry.totals;
        let _ = writeln!(
            out,
            "<tr><td>{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td>\
             <td class=\"n\">{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td>\
             <td class=\"n\">{}</td><td class=\"n\">{}</td></tr>",
            escape(&entry.subject),
            optional_count(totals.produce_requests),
            optional_rate(totals.produce_requests_per_million_acknowledged),
            optional_ratio(totals.records_per_produce_request),
            optional_ratio(totals.payload_bytes_per_transmitted_byte),
            optional_ratio(totals.batch_records.and_then(|window| window.mean())),
            optional_count(totals.retries),
            optional_count(totals.timeouts)
        );
    }
    let _ = writeln!(out, "</tbody></table>");
}
