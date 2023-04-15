package io.linkedlogics.kafka.process;

import java.util.Optional;

import io.linkedlogics.context.Context;
import io.linkedlogics.context.Status;
import io.linkedlogics.service.ServiceLocator;

public class ProcessTestHelper {
	public static final long MAX_WAIT_TIME = 500000;
	public static final long WAIT_SLEEP = 1;

	public static boolean waitUntil(String contextId, Status status) {
		return waitUntil(contextId, status, MAX_WAIT_TIME);
	}

	public static boolean waitUntil(String contextId, Status status, long maxWaitTime) {
		long start = System.currentTimeMillis();

		while (System.currentTimeMillis() - start < maxWaitTime) {

			Optional<Context> context = ServiceLocator.getInstance().getContextService().get(contextId);
			if (context.isEmpty()) {
				return false;
			} else if (context.get().getStatus() == status) {
				return true;
			} else {
//				System.out.println(context.get().getStatus());
			}
			
			try {
				Thread.sleep(WAIT_SLEEP);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		return false;
	}
}	
