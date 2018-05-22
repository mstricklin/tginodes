package com.ticomgeo.fame.nifi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ticomgeo.fame.nifi.dto.PushPointRequest;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.ticomgeo.fame.nifi.ResultEvents.PUSHPOINT_REQUEST_EVENT;
import static com.ticomgeo.fame.nifi.ResultEvents.TIMER_EVENT;

// ======== Node K =========

@EventDriven
@SideEffectFree
@Tags({"TGI", "PushPoint", "timer", "node", "NodeK"})
@CapabilityDescription("Non-Chronological Event")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = "fame.provider", description = "Node name"),
        @WritesAttribute(attribute = "fame.result", description = "Type of resulting event"),
        @WritesAttribute(attribute = "fame.ApiProtocol", description = "Protocol of resulting event"),
        @WritesAttribute(attribute = "fame.Production", description = "Production type of resulting event"),
        @WritesAttribute(attribute = "filename", description = "Required attribute: overloaded to identifier emitter"),
        @WritesAttribute(attribute = "timestamp.generated", description = "time this event was generated"),
        @WritesAttribute(attribute = "timestamp.data", description = "time the contained data was generated")
})
public class NodeK extends AbstractProcessor {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile ff = session.get();
        UUID.randomUUID().toString();

        FlowFile newFF = session.create(ff);
        session.remove(ff);

        newFF = session.putAllAttributes(newFF, staticAttributes);
        newFF = session.putAttribute(newFF, PushPointRequest.ID_KEY, UUID.randomUUID().toString());
        newFF = session.putAttribute(newFF, PushPointRequest.PAYLOAD_KEY, "Timer-based PushPoint Request");
        newFF = session.putAttribute(newFF, "Channel", PUSHPOINT_REQUEST_EVENT.getName());

        session.transfer(newFF, PUSHPOINT_REQUEST_EVENT);

        session.commit();
    }

    private static final Map<String,String> staticAttributes = ImmutableMap.of(
            "fame.provider", "NodeK",
            "fame.result", "PushPointRequestEvent",
            "fame.ApiProtocol", "fame",
            "fame.Production", "NonChronEvent");

    private Set<Relationship> relationships = ImmutableSet.of(TIMER_EVENT, PUSHPOINT_REQUEST_EVENT);

}
