package io.datanerds.newsfeed.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datanerds.newsfeed.domain.News;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class NewsSerializerTest {

    ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testSerialize() throws Exception {
        NewsSerializer serializer = new NewsSerializer();
        UUID uuid = UUID.randomUUID();
        News news = new News(uuid, "Test Author", "Test TitleÄÖÜ", "Test test test");
        byte[] bytes = serializer.serialize("", news);
        News reparsedNews = mapper.readValue(bytes, News.class);
        assertThat(reparsedNews.id, is(equalTo(news.id)));
        assertThat(reparsedNews.author, is(equalTo("Test Author")));
        assertThat(reparsedNews.title, is(equalTo("Test TitleÄÖÜ")));
        assertThat(reparsedNews.body, is(equalTo("Test test test")));

    }
}