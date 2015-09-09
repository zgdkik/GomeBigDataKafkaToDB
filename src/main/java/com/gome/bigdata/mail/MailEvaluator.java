package com.gome.bigdata.mail;

import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.TriggeringEventEvaluator;

public class MailEvaluator implements TriggeringEventEvaluator {

	@Override
	public boolean isTriggeringEvent(LoggingEvent loggingevent) {
		return loggingevent.getLevel().isGreaterOrEqual(Level.ERROR);
	}

}
