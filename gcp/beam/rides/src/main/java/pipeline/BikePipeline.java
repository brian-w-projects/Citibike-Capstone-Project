package pipeline;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

import java.sql.PreparedStatement;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;


public class BikePipeline {

  static class RidesExtract extends DoFn<String, String> {

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {

      String[] words = element.split(",");
      LocalDateTime temp = LocalDateTime.parse(words[1], DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss"));

      receiver.output(temp.format(DateTimeFormatter.ofPattern("uuuu-MM-dd HH")) + ":00:00");
    }
  }

  static class WeatherExtract extends DoFn<String, KV<java.sql.Timestamp, String>> {

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<KV<java.sql.Timestamp, String>> receiver) {

      String[] words = element.split(",");
      LocalDateTime temp = LocalDateTime.parse(words[0], DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss"));

      receiver.output(
              KV.of(java.sql.Timestamp.valueOf(temp.format(DateTimeFormatter.ofPattern("uuuu-MM-dd HH")) + ":00:00"),
                    element.substring(element.indexOf(",")+1)
              )
      );
    }
  }


  static class StringToTableRow extends DoFn<String, TableRow> {

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<TableRow> receiver) {

      String[] words = element.split(",");

      TableRow row = new TableRow()
              .set("start_time", words[1])
              .set("stop_time", words[2])
              .set("start_station_id", words[3])
              .set("end_station_id", words[7])
              .set("bike_id", words[11])
              .set("user_type", words[12])
              .set("gender", words[14]);

      if(!words[13].equals("\\N")){
          row.set("birth_year", words[13]);
      }

      receiver.output(row);
    }
  }


  public interface BikePipelineOptions extends DataflowPipelineOptions {

      @Description("Big Query Table")
      @Required
      ValueProvider<String> getBigQueryTable();

      void setBigQueryTable(ValueProvider<String> value);

      @Description("My SQL Table")
      @Required
      ValueProvider<String> getMySqlTable();

      void setMySqlTable(ValueProvider<String> value);

  }


  public static void main(String[] args) {
    BikePipelineOptions options =
            PipelineOptionsFactory
                    .fromArgs(args)
                    .withValidation()
                    .as(BikePipelineOptions.class);

    options.setStreaming(true);

    Pipeline p = Pipeline.create(options);

    PCollection<String> rawRides = p.apply("ReadRides", PubsubIO.readStrings().fromTopic("projects/capstone-231016/topics/rides"));

    // Write raw rides data to BigQuery
    rawRides.apply("StringToTableRow", ParDo.of(new StringToTableRow()))
            .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                    .to(options.getBigQueryTable())
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            );


    // Calc num of rides per hour and save into mysql

    rawRides.apply(ParDo.of(new RidesExtract()))
            .apply(Window.<String>into(TimeWindow.factory())
                    .withAllowedLateness(Duration.standardMinutes(10))
                    .accumulatingFiredPanes()
            )
            .apply(Count.<String>perElement())
            .apply(JdbcIO.<KV<String, Long>>write()
                      .withDataSourceConfiguration(
                              JdbcIO.DataSourceConfiguration.create("com.mysql.jdbc.Driver",
                                    options.getMySqlTable().get()
                              )
                      )
                      .withStatement("INSERT INTO Rides (`rides`, `date`) VALUES(?, ?) ON DUPLICATE KEY UPDATE rides=?")
                      .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<String, Long>>() {
                          @Override
                          public void setParameters(KV<String, Long> element, PreparedStatement query) throws Exception {
                              query.setLong(1, element.getValue());
                              query.setTimestamp(2, java.sql.Timestamp.valueOf(element.getKey()));
                              query.setLong(3, element.getValue());
                          }
                      })
            );

    // Pull weather and save into mysql
    p.apply("ReadWeather", PubsubIO.readStrings().fromTopic("projects/capstone-231016/topics/weather"))
            .apply(ParDo.of(new WeatherExtract()))
            .apply( JdbcIO.<KV<java.sql.Timestamp, String>>write()
                    .withDataSourceConfiguration(
                            JdbcIO.DataSourceConfiguration.create("com.mysql.jdbc.Driver",
                                    options.getMySqlTable().get()
                            )
                    )
                    .withStatement("INSERT IGNORE INTO Rides(`sunrise`,`precip`,`temp`,`humidity`,`windspeed`,`date`) VALUES(?, ?, ?, ?, ?, ?) ")
                    .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<java.sql.Timestamp, String>>() {
                        @Override
                        public void setParameters(KV<java.sql.Timestamp, String> element, PreparedStatement query) throws SQLException {
                            String[] elements = element.getValue().split(",");
                            query.setInt(1, Integer.parseInt(elements[0]));
                            query.setDouble(2, Double.parseDouble(elements[1]));
                            query.setDouble(3, Double.parseDouble(elements[2]));
                            query.setDouble(4, Double.parseDouble(elements[3]));
                            query.setDouble(5, Double.parseDouble(elements[4]));
                            query.setTimestamp(6, element.getKey());
                        }
                    })
            );


    p.run().waitUntilFinish();
  }
}

class TimeWindow extends WindowFn<Object, IntervalWindow> {

    final Duration size = Duration.standardMinutes(60);

    static TimeWindow factory(){
        return new TimeWindow();
    }

    @Override
    public Collection<IntervalWindow> assignWindows(AssignContext c) throws Exception {

        Instant start = c.timestamp().toDateTime().hourOfDay().roundFloorCopy().toInstant();
        return Arrays.asList(new IntervalWindow(start, size));
    }

    @Override
    public void mergeWindows(MergeContext c) throws Exception {

    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
        return false;
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
        return IntervalWindow.getCoder();
    }

    @Override
    public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
        return null;
    }
}