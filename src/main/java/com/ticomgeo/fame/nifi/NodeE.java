package com.ticomgeo.fame.nifi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.util.*;

import static com.ticomgeo.fame.nifi.ResultEvents.TIMER_EVENT;

// ======== Node E =========

@TriggerWhenEmpty
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "5 sec")
@SideEffectFree
@Tags({"TGI", "event", "timer", "node", "NodeE"})
@CapabilityDescription("Periodic Chronological Event")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = "fame.provider", description = "Node name"),
        @WritesAttribute(attribute = "fame.result", description = "Type of resulting event"),
        @WritesAttribute(attribute = "fame.ApiProtocol", description = "Protocol of resulting event"),
        @WritesAttribute(attribute = "fame.Production", description = "Production type of resulting event"),
        @WritesAttribute(attribute = "filename", description = "Required attribute: overloaded to identifier emitter"),
        @WritesAttribute(attribute = "timestamp.generated", description = "time this event was generated"),
        @WritesAttribute(attribute = "timestamp.data", description = "time the contained data was generated")
})
public class NodeE extends AbstractProcessor {
    // NodeE

    public static final PropertyDescriptor PERIOD = new PropertyDescriptor.Builder()
            .name("Event period - sec")
            .required(false)
            .defaultValue("5")
            .description("Period, in seconds, between emitted events.")
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    @Override
    protected void init(final ProcessorInitializationContext context) {
        name = getIdentifier() + " Event ";
    }
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        context.getControllerServiceLookup();

        FlowFile ff = session.create();

        Long epoch = System.currentTimeMillis();

        ff = session.putAllAttributes(ff, staticAttributes);
        ff = session.putAttribute(ff, CoreAttributes.FILENAME.key(),
                name+epoch.toString());

        ff = session.putAttribute(ff, "timestamp.generated",
                epoch.toString());
        ff = session.putAttribute(ff, "timestamp.data",
                epoch.toString());
        session.transfer(ff, TIMER_EVENT);

        session.commit();
    }

    private static final Map<String,String> staticAttributes = ImmutableMap.of(
            "fame.provider", "NodeE",
            "fame.result", "TimerEvent",
            "fame.ApiProtocol", "fame",
            "fame.Production", "PeriodChronEvent");
    private List<PropertyDescriptor> properties = ImmutableList.of(PERIOD);
    private Set<Relationship> relationships = ImmutableSet.of(TIMER_EVENT);
    private String name;

}
