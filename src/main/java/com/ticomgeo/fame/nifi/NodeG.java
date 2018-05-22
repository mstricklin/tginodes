package com.ticomgeo.fame.nifi;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ticomgeo.fame.nifi.dto.PushPointRequest;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ticomgeo.fame.nifi.ResultEvents.*;

// ======== Node G =========

@EventDriven
@SideEffectFree
@Tags({"TGI", "Geo", "filter", "node", "NodeG"})
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
public class NodeG extends AbstractProcessor {
    // UIGeoLocationProvider

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

        // incoming pushpoint request
        if (ff.getAttribute("fame.provider").equalsIgnoreCase("NodeK")) {
            PushPointRequest ppr = PushPointRequest.of(ff.getAttribute(PushPointRequest.ID_KEY),
                    ff.getAttribute(PushPointRequest.PAYLOAD_KEY));
            requestCache.put(ppr.id, ppr);

            FlowFile newFF = session.clone(ff);
            session.remove(ff);
            newFF = session.putAllAttributes(newFF, staticAttributes);

            // send on to geolocator
            session.transfer(newFF, UI_GEOLOCATION_QUERY);

        } else if (ff.getAttribute("fame.provider").equalsIgnoreCase("NodeD")) {
            // incoming response to query...look up initial request, if any...
            String pprID = ff.getAttribute(PushPointRequest.ID_KEY);
            PushPointRequest ppr = requestCache.getIfPresent(pprID);

            // Do something if we don't find the matching PP Request...

            FlowFile newFF = session.clone(ff);
            session.remove(ff);
            newFF = session.putAllAttributes(newFF, staticAttributes);

            // send on to UIEvent
            session.transfer(newFF, UI_EVENT_GEO);
        }

        session.commit();
    }

    private static final Map<String,String> staticAttributes = ImmutableMap.of(
            "fame.provider", "NodeG",
            "fame.result", "UIGeoLocation",
            "fame.ApiProtocol", "fame",
            "fame.Production", "NonChronEvent");

    private List<PropertyDescriptor> properties = ImmutableList.of();
    private Set<Relationship> relationships = ImmutableSet.of(
            PUSHPOINT_REQUEST_EVENT,
            UI_GEOLOCATION_QUERY, UI_GEOLOCATION_RESPONSE,
            UI_EVENT_GEO);

    Cache<String, PushPointRequest> requestCache = CacheBuilder.newBuilder()
            .maximumSize(100)
                .build();
}
