package com.ticomgeo.fame.nifi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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

import static com.ticomgeo.fame.nifi.ResultEvents.*;

@EventDriven
@SideEffectFree
@Tags({"TGI", "Geo", "node", "NodeC"})
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
public class NodeC extends AbstractProcessor {
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

        FlowFile newFF = session.clone(ff);
        session.remove(ff);
        newFF = session.putAllAttributes(newFF, staticAttributes);

        session.transfer(newFF, APP_GEO_RESPONSE);

        session.commit();
    }

    private static final Map<String, String> staticAttributes = ImmutableMap.of(
            "fame.provider", "NodeC",
            "fame.result", "UIGeoLocation",
            "fame.ApiProtocol", "fame",
            "fame.Production", "Query");

    private Set<Relationship> relationships = ImmutableSet.of(
            APP_GEO_QUERY, APP_GEO_RESPONSE);

}
