package com.practice.betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.json.*;
import com.practice.betterreadsdataloader.author.Author;
import com.practice.betterreadsdataloader.author.AuthorRepository;
import com.practice.betterreadsdataloader.author.Book;
import com.practice.betterreadsdataloader.author.BookRepository;

import Connection.DataStaxAstraProperties;
import jakarta.annotation.PostConstruct;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterReadsDataLoaderApplication {
	@Lazy
	@Autowired
	AuthorRepository authorrepository;
    
	@Lazy
	@Autowired
	BookRepository bookrepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BetterReadsDataLoaderApplication.class, args);
	}

	private void initAuthors() {
		Path path = Paths.get(authorDumpLocation);
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					Author author = new Author();
					author.setName(jsonObject.getString("name"));
					author.setPersonalName(jsonObject.getString("name"));
					author.setId(jsonObject.getString("key").replace("/authors/", ""));
					authorrepository.save(author);
				} catch (JSONException e) {
					e.printStackTrace();
				}

			});
			System.out.println("All Author Saved....");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void initWorks() {

		Path path = Paths.get(worksDumpLocation);
		DateTimeFormatter dateFormatter=DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					Book book = new Book();
					book.setId(jsonObject.optString("key").replace("/works/", ""));
					book.setName(jsonObject.optString("title"));
					JSONObject decjsonObject = jsonObject.optJSONObject("description");
					if (decjsonObject != null) {
						book.setDescription(decjsonObject.optString("value"));
					}
					JSONObject datejsonObject = jsonObject.optJSONObject("created");
					if (datejsonObject != null) {
						String datestr = datejsonObject.getString("value");
						book.setPublishedDate(LocalDate.parse(datestr,dateFormatter));
					}
					JSONArray coverjsonObject = jsonObject.optJSONArray("covers");
					if (coverjsonObject != null) {
						List<String> coverIds = new ArrayList<>();
						for (int i = 0; i < coverjsonObject.length(); i++) {
							coverIds.add(coverjsonObject.getString(i));
						}
						book.setCoverIds(coverIds);
					}

					JSONArray authorjsonObject = jsonObject.optJSONArray("authors");
					if (coverjsonObject != null) {
						List<String> authorIds = new ArrayList<>();
						for (int i = 0; i < authorjsonObject.length(); i++) {
							String authorId = authorjsonObject.getJSONObject(i).getJSONObject("author").getString("key")
									.replace("/authors/", "");
							authorIds.add(authorId);
						}
						book.setAuthorId(authorIds);
						List<String> authorNames = authorIds.stream().map(id -> authorrepository.findById(id))
								.map(optionalAuthor -> {
									if (!optionalAuthor.isPresent())
										return "Unknown Author";
									return optionalAuthor.get().getName();
								}).collect(Collectors.toList());
						book.setAuthorNames(authorNames);
						bookrepository.save(book);
					}
					System.out.println("All Book Saved....");
				} catch (JSONException e) {
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@PostConstruct
	public void start() {
		initAuthors();
		initWorks();
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
