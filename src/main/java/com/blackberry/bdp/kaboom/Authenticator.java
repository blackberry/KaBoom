/** Copyright 2014 BlackBerry, Limited.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.blackberry.bdp.kaboom;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Authenticator {

	private static final Logger LOG = LoggerFactory.getLogger(Authenticator.class);
	private final Map<String, UGIState> proxyUserMap;
	private final Object lock = new Object();

	private Authenticator() {
		proxyUserMap = new HashMap<>();
	}

	private static class SingletonHolder {

		public static final Authenticator INSTANCE = new Authenticator();

	}

	public static Authenticator getInstance() {
		return SingletonHolder.INSTANCE;
	}

	private boolean authenticate(String proxyUserName) throws IOException {
		UserGroupInformation proxyTicket;

		// logic for kerberos login
		boolean useSecurity = UserGroupInformation.isSecurityEnabled();

		LOG.info("Hadoop Security enabled: " + useSecurity);

		if (useSecurity) {
			// We now expect that all the logins are handled externally
			if (UserGroupInformation.getLoginUser() == null) {
				throw new IOException("current logged in super user not found");
			}
		}

		// hadoop impersonation works with or without kerberos security
		proxyTicket = null;

		if (!proxyUserName.isEmpty()) {
			try {
				proxyTicket = UserGroupInformation.createProxyUser(proxyUserName,
					 UserGroupInformation.getLoginUser());
			} catch (IOException e) {
				LOG.error("Unable to login as proxy user. Exception follows.", e);
				return false;
			}
		}

		UserGroupInformation ugi = null;
		if (proxyTicket != null) {
			ugi = proxyTicket;
		} else {
			if (useSecurity) {
				try {
					ugi = UserGroupInformation.getLoginUser();
				} catch (IOException e) {
					LOG.error("Unexpected error: Unable to get authenticated user after "
						 + "apparent successful login! Exception follows.", e);
					return false;
				}
			}
		}

		if (ugi != null) {
			AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
			LOG.info("Auth method: {}", authMethod);
			LOG.info(" User name: {}", ugi.getUserName());
			LOG.info(" Using keytab: {}", ugi.isFromKeytab());
			if (authMethod == AuthenticationMethod.PROXY) {
				UserGroupInformation superUser;
				try {
					superUser = UserGroupInformation.getLoginUser();
					LOG.info(" Superuser auth: {}", superUser.getAuthenticationMethod());
					LOG.info(" Superuser name: {}", superUser.getUserName());
					LOG.info(" Superuser using keytab: {}", superUser.isFromKeytab());
				} catch (IOException e) {
					LOG.error("Unexpected error: unknown superuser impersonating proxy.",
						 e);
					return false;
				}
			}

			LOG.info("Logged in as user {}", ugi.getUserName());

			UGIState state = new UGIState();
			state.ugi = proxyTicket;
			state.lastAuthenticated = System.currentTimeMillis();
			proxyUserMap.put(proxyUserName, state);

			return true;
		}

		return true;
	}

	/**
	 * Allow methods to act as another user (typically used for HDFS Kerberos)
	 *
	 * @param <T>
	 * @param proxyUser
	 * @param action
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public <T> T runPrivileged(final String proxyUser,
		 final PrivilegedExceptionAction<T> action) throws IOException,
		 InterruptedException {

		UGIState state = null;
		synchronized (lock) {
			state = proxyUserMap.get(proxyUser);
			if (state == null) {
				authenticate(proxyUser);
				state = proxyUserMap.get(proxyUser);
			}
		}

		UserGroupInformation proxyTicket = state.ugi;

		if (proxyTicket != null) {
			LOG.debug("Using proxy ticket {}", proxyTicket);
			try {
				return proxyTicket.doAs(action);
			} catch (IOException e) {
				LOG.error("Caught IOException while performing a privileged action: ", e);
				throw e;
			} catch (InterruptedException e) {
				LOG.error("Caught InterruptedException while performing a privileged action: ", e);
				throw e;
			}
		} else {
			try {
				return action.run();
			} catch (IOException | InterruptedException | RuntimeException ex) {
				throw ex;
			} catch (Exception ex) {
				throw new RuntimeException("Unexpected exception.", ex);
			}
		}
	}

	private class UGIState {
		public UserGroupInformation ugi = null;
		public long lastAuthenticated = 0l;
	}

}
