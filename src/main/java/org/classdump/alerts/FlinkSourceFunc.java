package org.classdump.alerts;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;

import static org.classdump.alerts.LoginEvent.event;

public class FlinkSourceFunc implements SourceFunction<Event> {

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        long now = 1;
        for (int i = 0; i < 100000 ; i++) {
            final ArrayList<Event> failedEvents =
                    Lists.newArrayList(
                            event(now++).prop("username", "A").prop("type", "failed"),
                            event(now++).prop("username", "C").prop("type", "failed"),
                            event(now++).prop("username", "A").prop("type", "failed"),
                            event(now++).prop("username", "C").prop("type", "failed"),
                            event(now++).prop("username", "A").prop("type", "failed"),
                            event(now++).prop("username", "C").prop("type", "failed"),
                            event(now++).prop("username", "B").prop("type", "failed"),
                            event(now++).prop("username", "A").prop("type", "failed"),
                            event(now++).prop("username", "C").prop("type", "failed"),
                            event(now++).prop("username", "C").prop("type", "failed"),
                            event(now++).prop("username", "A").prop("type", "failed"),
                            event(now++).prop("username", "A").prop("type", "failed"),
                            event(now++).prop("username", "A").prop("type", "failed"),

                            //event(now++).prop("username", "A").prop("type", "pw-reset"),

                            event(now++).prop("username", "B").prop("type", "success"),
                            event(now++).prop("username", "A").prop("type", "success"),
                            event(now++).prop("username", "A").prop("type", "success"),
                            event(now++).prop("username", "C").prop("type", "success")
            );

            for (Event event: failedEvents) {
                sourceContext.collectWithTimestamp(event, event.timestamp);
            }
            sourceContext.emitWatermark(new Watermark(now));
        }

    }
    @Override
    public void cancel() {

    }
}
