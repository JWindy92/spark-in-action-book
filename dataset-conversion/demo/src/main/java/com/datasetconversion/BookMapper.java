package com.datasetconversion;

import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class BookMapper implements MapFunction<Row, Book> {
    private static final long serialVersionUID = -2L;

    public Book call(Row value) throws Exception {
      Book b = new Book();
      Integer id = value.getAs("id");
      Integer authorId = value.getAs("authorId");
      String link = value.getAs("link");
      String title = value.getAs("title");

      b.setId(id);
      b.setAuthorId(authorId);
      b.setLink(link);
      b.setTitle(title);

      // date case
      String dateAsString = value.getAs("releaseDate");
      if (dateAsString != null) {
        SimpleDateFormat parser = new SimpleDateFormat("M/d/yy");
        b.setReleaseDate(parser.parse(dateAsString));
      }
      return b;
    }
  }
