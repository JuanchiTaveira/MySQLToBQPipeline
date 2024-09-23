package org.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        String driverConnection = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql:///dart_db?cloudSqlInstance=sunny-might-435812-p7:europe-west1:dart" +
                "&socketFactory=com.google.cloud.sql.mysql.SocketFactory" +
                "&user=dart_user" +
                "&password=dart_user";
        String username = "dart_user";
        String password = "dart_user";
        String outputDataset = "dart_test";
        String outputTable = "dart_table_bq";
        String projectOutput = "sunny-might-435812-p7";
        String outputBucket = "dart_test_bucket_124";

        TableSchema schema = new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("cob_date_code").setType("STRING"),
                new TableFieldSchema().setName("bus_desc_l6").setType("STRING"),
                new TableFieldSchema().setName("scen_code").setType("STRING"),
                new TableFieldSchema().setName("amnt_code").setType("STRING"),
                new TableFieldSchema().setName("amount").setType("FLOAT")
        ));

        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        pipelineOptions.setProject(projectOutput);
        pipelineOptions.setRegion("us-central1");
        pipelineOptions.setTempLocation("gs://" + outputBucket + "/temp");
        pipelineOptions.setRunner(DataflowRunner.class);
        pipelineOptions.setNumWorkers(2);
        pipelineOptions.setMaxNumWorkers(5);
        pipelineOptions.setWorkerMachineType("n1-standard-1");
        pipelineOptions.setAutoscalingAlgorithm(DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED);

        Pipeline p = Pipeline.create(pipelineOptions);
        p.apply("Read from MySQL", JdbcIO.<TableRow>read()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(driverConnection, url)
                                .withUsername(username)
                                .withPassword(password))
                        .withQuery("SELECT * FROM dart_table")
                        .withRowMapper((JdbcIO.RowMapper<TableRow>) rs -> {
                            TableRow tableRow = new TableRow();
                            tableRow.set("cob_date_code", rs.getString("cob_date_code"));
                            tableRow.set("bus_desc_l6", rs.getString("bus_desc_l6"));
                            tableRow.set("scen_code", rs.getString("scen_code"));
                            tableRow.set("amnt_code", rs.getString("amnt_code"));
                            tableRow.set("amount", rs.getDouble("amount"));

                            return tableRow;
                        })
                        .withCoder(TableRowJsonCoder.of())
                )
                .apply("Input log", ParDo.of(new DoFn<TableRow, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        TableRow row = c.element();
                        log.info("Reading line: {}", row);
                        c.output(row);
                    }
                }))
                .apply("Write into Big Query", BigQueryIO.writeTableRows()
                        .to(projectOutput + "." + outputDataset + "." + outputTable)
                        .withSchema(schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of("gs://" + outputBucket + "/bigquery_temp"))
                );

        p.run().waitUntilFinish();
    }
}