package org.apache.flink.cdc.cli;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.cdc.cli.parser.PipelineDefinitionParser;
import org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator;
import org.apache.flink.cdc.composer.flink.translator.*;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class FlinkApp {
    /**
     * @author dingtianlu
     * @date 2024-07-01 17:13
     */
    public static void main(String[] args) throws Exception {
        System.out.println("flink cdc starting...");
        if (args.length == 0) {
            throw new IllegalArgumentException(
                    "Missing pipeline definition file path in arguments. ");
        }
        Path pipelineDefPath = Paths.get(args[0]);
        if (!Files.exists(pipelineDefPath)) {
            throw new FileNotFoundException(
                    String.format("Cannot find pipeline definition file \"%s\"", pipelineDefPath));
        }
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                pipelineDefinitionParser.parse(pipelineDefPath, new Configuration());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //    设置每3s生成一次checkpoint
        env.enableCheckpointing(3000);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        5, // 尝试重启的次数
                        Time.of(10, TimeUnit.SECONDS) // 间隔
                        ));
        int parallelism = pipelineDef.getConfig().get(PipelineOptions.PIPELINE_PARALLELISM);
        env.getConfig().setParallelism(parallelism);
        // Build Source Operator
        DataSourceTranslator sourceTranslator = new DataSourceTranslator();
        DataStream<Event> stream =
                sourceTranslator.translate(pipelineDef.getSource(), env, pipelineDef.getConfig());

        // Build TransformSchemaOperator for processing Schema Event
        TransformTranslator transformTranslator = new TransformTranslator();
        stream = transformTranslator.translateSchema(stream, pipelineDef.getTransforms());

        // Schema operator
        SchemaOperatorTranslator schemaOperatorTranslator =
                new SchemaOperatorTranslator(
                        pipelineDef
                                .getConfig()
                                .get(PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR),
                        pipelineDef.getConfig().get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID),
                        pipelineDef
                                .getConfig()
                                .get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT));
        OperatorIDGenerator schemaOperatorIDGenerator =
                new OperatorIDGenerator(schemaOperatorTranslator.getSchemaOperatorUid());

        // Build TransformDataOperator for processing Data Event
        stream =
                transformTranslator.translateData(
                        stream,
                        pipelineDef.getTransforms(),
                        schemaOperatorIDGenerator.generate(),
                        pipelineDef.getConfig().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));

        // Build DataSink in advance as schema operator requires MetadataApplier
        DataSink dataSink = createDataSink(pipelineDef.getSink(), pipelineDef.getConfig());

        stream =
                schemaOperatorTranslator.translate(
                        stream, parallelism, dataSink.getMetadataApplier(), pipelineDef.getRoute());

        // Build Partitioner used to shuffle Event
        PartitioningTranslator partitioningTranslator = new PartitioningTranslator();
        stream =
                partitioningTranslator.translate(
                        stream, parallelism, parallelism, schemaOperatorIDGenerator.generate());

        // Build Sink Operator
        DataSinkTranslator sinkTranslator = new DataSinkTranslator();
        sinkTranslator.translate(
                pipelineDef.getSink(), stream, dataSink, schemaOperatorIDGenerator.generate());
        String jobName = pipelineDef.getConfig().get(PipelineOptions.PIPELINE_NAME);
        env.execute(jobName);
    }

    private static DataSink createDataSink(SinkDef sinkDef, Configuration pipelineConfig) {
        // Search the data sink factory
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier(
                        sinkDef.getType(), DataSinkFactory.class);

        // Create data sink
        return sinkFactory.createDataSink(
                new FactoryHelper.DefaultContext(
                        sinkDef.getConfig(),
                        pipelineConfig,
                        Thread.currentThread().getContextClassLoader()));
    }
}
