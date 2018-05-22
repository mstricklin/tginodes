package com.ticomgeo.fame.nifi;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;


/**
 * @author mstricklin
 * @version 1.0
 */

@EventDriven
@SideEffectFree
@Tags({"TGI", "logging"})
@CapabilityDescription("Log it ALL!!!")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class TGILogger extends AbstractProcessor {
	public static final PropertyDescriptor LOG_PREFIX = new PropertyDescriptor.Builder()
			.name("Log prefix")
			.required(false)
			.defaultValue("XXX")
			.description("Log prefix appended to the log lines. It helps to distinguish the output of multiple LogAttribute processors.")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(true)
			.build();
	public static final PropertyDescriptor LOG_DIRECTORY = new PropertyDescriptor.Builder()
			.name("Log Directory")
			.description("The log directory where log-files are written")
			.required(true)
			.defaultValue("/tmp")
			.addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
			.expressionLanguageSupported(true)
			.build();
	@SuppressWarnings("unused")
	private static final Logger CLASS_LOGGER = LoggerFactory.getLogger((new Throwable()).getStackTrace()[0].getClassName());

	@SuppressWarnings("unused")
	private static final String NEWLINE = System.getProperty("line.separator");

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("All FlowFiles are routed to this relationship")
			.build();

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(LOG_PREFIX);
		properties.add(LOG_DIRECTORY);
		this.properties = Collections.unmodifiableList(properties);

		final Set<Relationship> procRels = new HashSet<>();
		procRels.add(REL_SUCCESS);
		relationships = Collections.unmodifiableSet(procRels);
	}

	@Override
	public void onTrigger(ProcessContext context,
	                      ProcessSession session)
			throws ProcessException {
		final FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}
		final String filename = flowFile.getAttribute("filename");


		final String logPrefix = context.getProperty(LOG_PREFIX).getValue();
		CLASS_LOGGER.info("{} Log message {}", logPrefix, filename);
		session.transfer(flowFile, REL_SUCCESS);
	}
	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}
	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}



	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;


}