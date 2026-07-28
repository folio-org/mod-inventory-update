package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.folio.inventoryupdate.importing.moduledata.Channel;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.utils.SettableClock;

/**
 * Harvests source files from a simple HTTP HTML directory listing.
 *
 * <p>The channel supplies the index URL through {@code harvestUrl} and the cutoff timestamp through
 * {@code lastHarvested}. The harvester fetches the index page, discovers linked files, resolves each file's
 * modified timestamp, and pushes only files newer than {@code lastHarvested} to the provided {@link FileQueue}.
 * Eligible files are pushed in chronological order by modified timestamp, oldest first.</p>
 *
 * <p>Directory-index timestamps are preferred because they avoid extra file requests. If a listing has no parseable
 * timestamp for a file, the harvester falls back to the file's HTTP {@code Last-Modified} header. After all selected
 * files have been fetched and queued successfully, the channel's {@code lastHarvested} value is updated to the time
 * when the index page fetch was registered.</p>
 */
public class HtmlDirectoryHarvester {

  private static final Pattern LINK_PATTERN = Pattern.compile(
      "(?is)<a\\s+[^>]*href\\s*=\\s*([\"'])(.*?)\\1[^>]*>(.*?)</a>");
  private static final Pattern TAG_PATTERN = Pattern.compile("(?is)<[^>]+>");
  private static final Pattern ISO_DATE_TIME_PATTERN = Pattern.compile(
      "\\b(\\d{4}-\\d{2}-\\d{2})[ T](\\d{2}:\\d{2}(?::\\d{2})?)\\b");
  private static final Pattern APACHE_DATE_TIME_PATTERN = Pattern.compile(
      "\\b(\\d{2}-[A-Za-z]{3}-\\d{4})\\s+(\\d{2}:\\d{2})\\b");
  private static final Pattern SHORT_APACHE_DATE_TIME_PATTERN = Pattern.compile(
      "\\b(\\d{2}-[A-Za-z]{3}-\\d{2})\\s+(\\d{2}:\\d{2})\\b");
  private static final DateTimeFormatter DB_TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss,SSS");
  private static final DateTimeFormatter APACHE_DATE = new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .appendPattern("dd-MMM-yyyy")
      .toFormatter(Locale.ENGLISH);
  private static final DateTimeFormatter SHORT_APACHE_DATE = new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .appendPattern("dd-MMM-yy")
      .toFormatter(Locale.ENGLISH);

  private final WebClient webClient;

  public HtmlDirectoryHarvester(Vertx vertx) {
    this(WebClient.create(vertx, new WebClientOptions().setFollowRedirects(true)));
  }

  HtmlDirectoryHarvester(WebClient webClient) {
    this.webClient = webClient;
  }

  /**
   * Fetches files from HTML directory for the given channel.
   *
   * <p>If the channel has no {@code harvestUrl}, this returns a succeeded future with {@code null}. If the index
   * page fetch, a selected source-file fetch, a queue push, or the final {@code lastHarvested} update fails, the
   * returned future fails. Files pushed before a later failure are not rolled back.</p>
   *
   * @param channel channel containing harvest configuration and state
   * @param fileQueue queue receiving harvested source files
   * @param db storage used to persist the successful harvest timestamp
   * @return harvest counts and the new {@code lastHarvested} value, or {@code null} when no harvest URL is configured
   */
  public Future<HarvestResult> harvest(Channel channel, FileQueue fileQueue, EntityStorage db) {
    String harvestUrl = channel.getRecord().harvestUrl();
    if (harvestUrl == null || harvestUrl.isBlank()) {
      return Future.succeededFuture(null);
    }
    Instant previousHarvest = parseTimestamp(channel.getRecord().lastHarvested()).orElse(Instant.MIN);
    String registeredFetchTime = dbTimestamp(SettableClock.getLocalDateTime());
    URI indexUri = URI.create(harvestUrl);

    return webClient.getAbs(harvestUrl).send()
        .compose(indexResponse -> {
          if (!successful(indexResponse)) {
            return Future.failedFuture("Failed to fetch harvest index " + harvestUrl
                + ": HTTP " + indexResponse.statusCode());
          }
          List<DirectoryEntry> entries = parseDirectoryIndex(indexUri, indexResponse.bodyAsString());
          HarvestStats statistics = new HarvestStats(entries.size());
          // Resolve timestamps before fetching payloads so queue insertion can be oldest-file-first.
          return collectNewerEntries(entries, 0, previousHarvest, statistics, new ArrayList<>())
              .compose(timedEntries -> {
                timedEntries.sort(Comparator.comparing(TimedDirectoryEntry::timestamp)
                    .thenComparing(entry -> entry.entry().url()));
                channel.setLastHarvested(registeredFetchTime, db);
                return fetchAndPushFilesToQueue(timedEntries, 0, fileQueue, statistics);
              })
              .map(result -> result.withLastHarvested(registeredFetchTime));
        });
  }

  /**
   * Resolves each entry's timestamp and keeps only files newer than the previous successful harvest.
   */
  private Future<List<TimedDirectoryEntry>> collectNewerEntries(List<DirectoryEntry> entries, int position,
                                                               Instant previousHarvest,
                                                               HarvestStats statistics,
                                                               List<TimedDirectoryEntry> newEntries) {
    if (position >= entries.size()) {
      return Future.succeededFuture(newEntries);
    }
    DirectoryEntry entry = entries.get(position);
    Future<Optional<Instant>> timestamp = entry.timestamp().isPresent()
        ? Future.succeededFuture(entry.timestamp())
        : fetchLastModified(entry.url());
    return timestamp.compose(fileTimestamp -> {
      if (fileTimestamp.isEmpty()) {
        statistics.skippedNoTimestamp++;
        return collectNewerEntries(entries, position + 1, previousHarvest, statistics, newEntries);
      }
      if (!fileTimestamp.get().isAfter(previousHarvest)) {
        statistics.skippedOld++;
        return collectNewerEntries(entries, position + 1, previousHarvest, statistics, newEntries);
      }
      newEntries.add(new TimedDirectoryEntry(entry, fileTimestamp.get()));
      return collectNewerEntries(entries, position + 1, previousHarvest, statistics, newEntries);
    });
  }

  /**
   * Fetches and pushes already-sorted entries sequentially.
   */
  private Future<HarvestResult> fetchAndPushFilesToQueue(List<TimedDirectoryEntry> entries, int position,
                                                         FileQueue fileQueue, HarvestStats statistics) {
    if (position >= entries.size()) {
      return Future.succeededFuture(statistics.result());
    }
    DirectoryEntry entry = entries.get(position).entry();
    return fetchFile(entry.url())
        .compose(payload -> fileQueue.push(entry.fileName(),
            entry.timestamp().isPresent() // Should never be not present.
                ? String.valueOf(entry.timestamp().get()) : SettableClock.getLocalDateTime().toString(),
            payload))
        .compose(na -> {
          statistics.queued++;
          return fetchAndPushFilesToQueue(entries, position + 1, fileQueue, statistics);
        });
  }

  /**
   * Gets a file's HTTP Last-Modified timestamp. Some servers do not support HEAD, so a GET is used as fallback.
   */
  private Future<Optional<Instant>> fetchLastModified(String fileUrl) {
    return webClient.headAbs(fileUrl).send()
        .compose(response -> {
          if (successful(response)) {
            Optional<Instant> timestamp = parseHttpDate(response.getHeader("Last-Modified"));
            if (timestamp.isPresent()) {
              return Future.succeededFuture(timestamp);
            }
          }
          return webClient.getAbs(fileUrl).send()
              .map(getResponse -> parseHttpDate(getResponse.getHeader("Last-Modified")));
        });
  }

  private Future<String> fetchFile(String fileUrl) {
    return webClient.getAbs(fileUrl).send()
        .compose(response -> {
          if (!successful(response)) {
            return Future.failedFuture("Failed to fetch harvested file " + fileUrl
                + ": HTTP " + response.statusCode());
          }
          return Future.succeededFuture(response.bodyAsString());
        });
  }

  /**
   * Parses file links from common Apache/nginx-style HTML directory indexes.
   *
   * <p>The returned entries are URL-sorted only for deterministic discovery order. Harvest order is determined later,
   * after all modified timestamps have been resolved.</p>
   */
  public static List<DirectoryEntry> parseDirectoryIndex(URI indexUri, String html) {
    List<DirectoryEntry> entries = new ArrayList<>();
    Matcher matcher = LINK_PATTERN.matcher(html);
    while (matcher.find()) {
      String href = decodeHtml(matcher.group(2)).trim();
      if (href.isEmpty() || href.startsWith("?") || href.startsWith("#")) {
        continue;
      }
      URI resolved = indexUri.resolve(href);
      String fileName = fileName(resolved);
      if (fileName == null || fileName.equals("..") || href.endsWith("/") || resolved.getPath().endsWith("/")) {
        continue;
      }
      String context = linkContext(html, matcher.end());
      entries.add(new DirectoryEntry(resolved.toString(), fileName, parseDirectoryTimestamp(context)));
    }
    entries.sort(Comparator.comparing(DirectoryEntry::url));
    return entries;
  }

  /**
   * Extracts the visible text after a link, where directory listings usually place timestamp and size columns.
   */
  private static String linkContext(String html, int linkEnd) {
    int nextLink = html.length();
    Matcher next = LINK_PATTERN.matcher(html);
    if (next.find(linkEnd)) {
      nextLink = next.start();
    }
    int nextLine = html.indexOf('\n', linkEnd);
    int end = nextLine < 0 ? nextLink : Math.min(nextLine, nextLink);
    return decodeHtml(TAG_PATTERN.matcher(html.substring(linkEnd, end)).replaceAll(" "));
  }

  /**
   * Parses directory-listing timestamp formats seen in common HTTP directory indexes.
   */
  private static Optional<Instant> parseDirectoryTimestamp(String text) {
    Matcher iso = ISO_DATE_TIME_PATTERN.matcher(text);
    if (iso.find()) {
      return parseTimestamp(iso.group(1) + "T" + normalizeTime(iso.group(2)));
    }
    Matcher apache = APACHE_DATE_TIME_PATTERN.matcher(text);
    if (apache.find()) {
      return parseDateAndTime(apache.group(1), apache.group(2), APACHE_DATE);
    }
    Matcher shortApache = SHORT_APACHE_DATE_TIME_PATTERN.matcher(text);
    if (shortApache.find()) {
      return parseDateAndTime(shortApache.group(1), shortApache.group(2), SHORT_APACHE_DATE);
    }
    return Optional.empty();
  }

  /**
   * Parses stored channel timestamps. Values without an offset are treated as UTC.
   */
  public static Optional<Instant> parseTimestamp(String value) {
    if (value == null || value.isBlank()) {
      return Optional.empty();
    }
    String normalized = value.trim().replace(',', '.');
    try {
      return Optional.of(OffsetDateTime.parse(normalized).toInstant());
    } catch (DateTimeParseException ignored) {
      // Try local timestamp formats below.
    }
    try {
      return Optional.of(LocalDateTime.parse(normalized).toInstant(ZoneOffset.UTC));
    } catch (DateTimeParseException ignored) {
      return Optional.empty();
    }
  }

  /**
   * Parses an HTTP Last-Modified header.
   */
  public static Optional<Instant> parseHttpDate(String value) {
    if (value == null || value.isBlank()) {
      return Optional.empty();
    }
    try {
      return Optional.of(DateTimeFormatter.RFC_1123_DATE_TIME.parse(value, Instant::from));
    } catch (DateTimeParseException ignored) {
      return Optional.empty();
    }
  }

  private static Optional<Instant> parseDateAndTime(String date, String time, DateTimeFormatter dateFormatter) {
    try {
      return Optional.of(LocalDateTime.of(
          LocalDate.parse(date, dateFormatter),
          LocalTime.parse(normalizeTime(time), DateTimeFormatter.ISO_LOCAL_TIME)).toInstant(ZoneOffset.UTC));
    } catch (DateTimeParseException ignored) {
      return Optional.empty();
    }
  }

  private static String normalizeTime(String time) {
    return time.length() == 5 ? time + ":00" : time;
  }

  private static String dbTimestamp(LocalDateTime dateTime) {
    return dateTime.truncatedTo(ChronoUnit.MILLIS).format(DB_TIMESTAMP);
  }

  private static boolean successful(HttpResponse<Buffer> response) {
    return response.statusCode() >= 200 && response.statusCode() < 300;
  }

  private static String fileName(URI uri) {
    String path = uri.getPath();
    if (path == null || path.isBlank()) {
      return null;
    }
    int end = path.endsWith("/") ? path.length() - 1 : path.length();
    int start = path.lastIndexOf('/', end - 1) + 1;
    return URLDecoder.decode(path.substring(start, end), StandardCharsets.UTF_8);
  }

  private static String decodeHtml(String value) {
    return value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'");
  }

  public record DirectoryEntry(String url, String fileName, Optional<Instant> timestamp) {}

  private record TimedDirectoryEntry(DirectoryEntry entry, Instant timestamp) {}

  /**
   * Summary of one successful harvest cycle.
   */
  public record HarvestResult(int discoveredFiles, int queuedFiles, int skippedOldFiles,
                              int skippedFilesWithoutTimestamp, String lastHarvested) {
    HarvestResult withLastHarvested(String timestamp) {
      return new HarvestResult(discoveredFiles, queuedFiles, skippedOldFiles, skippedFilesWithoutTimestamp,
          timestamp);
    }
  }

  private static class HarvestStats {
    private final int discovered;
    private int queued;
    private int skippedOld;
    private int skippedNoTimestamp;

    HarvestStats(int discovered) {
      this.discovered = discovered;
    }

    HarvestResult result() {
      return new HarvestResult(discovered, queued, skippedOld, skippedNoTimestamp, null);
    }
  }
}
