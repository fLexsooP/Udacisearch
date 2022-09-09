package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final PageParserFactory parserFactory;
  private final int depth;
  private final List<Pattern> ignoredUrls;
  

  private final ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();
  private final ConcurrentHashMap<String, Integer> counts = new ConcurrentHashMap<>();

  @Inject
  ParallelWebCrawler(
          Clock clock,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int threadCount,
          PageParserFactory pageParserFactory,
          @MaxDepth int maxDepth,
          @IgnoredUrls List<Pattern> ignoredUrls) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.parserFactory = pageParserFactory;
    this.depth = maxDepth;
    this.ignoredUrls = ignoredUrls;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);
    for(String url : startingUrls) {
      if (isVisited(url) || isIgnore(url))
        continue;
      pool.invoke(new CrawlTask(url, depth, deadline));
    }
    return new CrawlResult.Builder()
            .setWordCounts(counts.isEmpty() ? counts : WordCounts.sort(counts, popularWordCount))
            .setvisitedUrls(visitedUrls.size())
            .build();
  }

  private boolean isIgnore(String url) {
    for (Pattern ignoredUrl : ignoredUrls) {
      if (ignoredUrl.matcher(url).matches())
        return true;
    }
    return false;
  }

  private boolean isVisited(String url) {
    return visitedUrls.contains(url);
  }


  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }

  private class CrawlTask extends RecursiveTask<Void> {

    private final String url;
    private final int crawlTaskDepth;
    Instant deadline;

    public CrawlTask(String url, int depth, Instant deadline) {
      this.url = url;
      this.crawlTaskDepth = depth;
      this.deadline = deadline;
    }

    @Override
    protected Void compute() {
      if(crawlTaskDepth == 0 || clock.instant().isAfter(deadline))
        return null;
      visitedUrls.add(url);
      PageParser.Result result = parserFactory.get(url).parse();
      for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
        if (counts.containsKey(e.getKey())) {
          counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
        } else {
          counts.put(e.getKey(), e.getValue());
        }
      }
      for(String url : result.getLinks()) {
        if (isVisited(url) || isIgnore(url))
          continue;
        pool.invoke(new CrawlTask(url, crawlTaskDepth -1, deadline));
      }
      return null;
    }
  }
}
