package org.apache.flink.streaming.examples.kjtest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class DataSourceUtil  extends RichParallelSourceFunction<Tuple2<String,Integer>> {

	private Boolean running = true;
	@Override
	public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
		Random random = new Random(System.currentTimeMillis());
		int count = 0;
		while (running) {
			Thread.sleep(1000);
			String key = "匡静测试" + count % 10;
			count++;
//			/System.out.println(key + ":" + count);
			ctx.collect(new Tuple2<>(key,count));
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}
