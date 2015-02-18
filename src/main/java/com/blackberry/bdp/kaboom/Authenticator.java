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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class Authenticator {
	private static final Logger LOG = LoggerFactory
			.getLogger(Authenticator.class);

	private String kerbConfPrincipal;
	private String kerbKeytab;
	/**
	 * Singleton credential manager that manages static credentials for the entire
	 * JVM
	 */
	private static final AtomicReference<KerberosUser> staticLogin = new AtomicReference<KerberosUser>();

	private Map<String, UGIState> proxyUserMap;
	private Object lock = new Object();

	private long reauthenticationRetryInterval = 10000;

	private Authenticator() {
		proxyUserMap = new HashMap<String, UGIState>();
	}

	private static class SingletonHolder {
		public static final Authenticator INSTANCE = new Authenticator();
	}

	public static Authenticator getInstance() {
		return SingletonHolder.INSTANCE;
	}

	public String getKerbConfPrincipal() {
		return kerbConfPrincipal;
	}

	public void setKerbConfPrincipal(String kerbConfPrincipal) {
		this.kerbConfPrincipal = kerbConfPrincipal;
	}

	public String getKerbKeytab() {
		return kerbKeytab;
	}

	public void setKerbKeytab(String kerbKeytab) {
		this.kerbKeytab = kerbKeytab;
	}

	/*
	 * The following methods were taken from the Apache Flume project, and are
	 * used under license.
	 * 
	 * Licensed to the Apache Software Foundation (ASF) under one or more
	 * contributor license agreements. See the NOTICE file distributed with this
	 * work for additional information regarding copyright ownership. The ASF
	 * licenses this file to you under the Apache License, Version 2.0 (the
	 * "License"); you may not use this file except in compliance with the
	 * License. You may obtain a copy of the License at
	 * 
	 * http://www.apache.org/licenses/LICENSE-2.0
	 * 
	 * Unless required by applicable law or agreed to in writing, software
	 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
	 * License for the specific language governing permissions and limitations
	 * under the License.
	 */
	private boolean authenticate(String proxyUserName) {
		UserGroupInformation proxyTicket;

		// logic for kerberos login
		boolean useSecurity = UserGroupInformation.isSecurityEnabled();

		LOG.info("Hadoop Security enabled: " + useSecurity);

		if (useSecurity) {
			// sanity checking
			if (kerbConfPrincipal.isEmpty()) {
				LOG.error("Hadoop running in secure mode, but Flume config doesn't "
						+ "specify a principal to use for Kerberos auth.");
				return false;
			}
			if (kerbKeytab.isEmpty()) {
				LOG.error("Hadoop running in secure mode, but Flume config doesn't "
						+ "specify a keytab to use for Kerberos auth.");
				return false;
			}

			String principal;
			try {
				// resolves _HOST pattern using standard Hadoop search/replace
				// via DNS lookup when 2nd argument is empty
				principal = SecurityUtil.getServerPrincipal(kerbConfPrincipal, "");
			} catch (IOException e) {
				LOG.error("Host lookup error resolving kerberos principal ("
						+ kerbConfPrincipal + "). Exception follows.", e);
				return false;
			}

			Preconditions.checkNotNull(principal, "Principal must not be null");
			KerberosUser prevUser = staticLogin.get();
			KerberosUser newUser = new KerberosUser(principal, kerbKeytab);

			// be cruel and unusual when user tries to login as multiple principals
			// this isn't really valid with a reconfigure but this should be rare
			// enough to warrant a restart of the agent JVM
			// TODO: find a way to interrogate the entire current config state,
			// since we don't have to be unnecessarily protective if they switch all
			// HDFS sinks to use a different principal all at once.
			Preconditions.checkState(prevUser == null || prevUser.equals(newUser),
					"Cannot use multiple kerberos principals in the same agent. "
							+ " Must restart agent to use new principal or keytab. "
							+ "Previous = %s, New = %s", prevUser, newUser);

			// attempt to use cached credential if the user is the same
			// this is polite and should avoid flooding the KDC with auth requests
			UserGroupInformation curUser = null;
			if (prevUser != null && prevUser.equals(newUser)) {
				try {
					curUser = UserGroupInformation.getLoginUser();
				} catch (IOException e) {
					LOG.warn("User unexpectedly had no active login. Continuing with "
							+ "authentication", e);
				}
			}

			if (curUser == null || !curUser.getUserName().equals(principal)) {
				try {
					// static login
					kerberosLogin(this, principal, kerbKeytab);
				} catch (IOException e) {
					LOG.error("Authentication or file read error while attempting to "
							+ "login as kerberos principal (" + principal + ") using "
							+ "keytab (" + kerbKeytab + "). Exception follows.", e);
					return false;
				}
			} else {
				LOG.debug("{}: Using existing principal login: {}", this, curUser);
			}

			try {
				if (UserGroupInformation.getLoginUser().isFromKeytab() == false) 
				{
					LOG.error("Using a keytab for authentication is {}, shutting down", UserGroupInformation.getLoginUser().isFromKeytab());
					LOG.info("Is the current user from keytab: {}", curUser.isFromKeytab());
					System.exit(1);
				}
			} catch (IOException e) {
				LOG.error("Failed to get login user.", e);
				System.exit(1);
			}

			// we supposedly got through this unscathed... so store the static user
			staticLogin.set(newUser);
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
		} else if (useSecurity) {
			try {
				ugi = UserGroupInformation.getLoginUser();
			} catch (IOException e) {
				LOG.error("Unexpected error: Unable to get authenticated user after "
						+ "apparent successful login! Exception follows.", e);
				return false;
			}
		}

		if (ugi != null) {
			// dump login information
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
	 * Static synchronized method for static Kerberos login. <br/>
	 * Static synchronized due to a thundering herd problem when multiple Sinks
	 * attempt to log in using the same principal at the same time with the
	 * intention of impersonating different users (or even the same user). If this
	 * is not controlled, MIT Kerberos v5 believes it is seeing a replay attach
	 * and it returns: <blockquote>Request is a replay (34) -
	 * PROCESS_TGS</blockquote> In addition, since the underlying Hadoop APIs we
	 * are using for impersonation are static, we define this method as static as
	 * well.
	 * 
	 * @param principal
	 *          Fully-qualified principal to use for authentication.
	 * @param keytab
	 *          Location of keytab file containing credentials for principal.
	 * @return Logged-in user
	 * @throws IOException
	 *           if login fails.
	 */
	private synchronized UserGroupInformation kerberosLogin(
			Authenticator authenticator, String principal, String keytab)
			throws IOException {

		// if we are the 2nd user thru the lock, the login should already be
		// available statically if login was successful
		UserGroupInformation curUser = null;
		try {
			curUser = UserGroupInformation.getLoginUser();
		} catch (IOException e) {
			// not a big deal but this shouldn't typically happen because it will
			// generally fall back to the UNIX user
			LOG.debug("Unable to get login user before Kerberos auth attempt.", e);
		}

		// we already have logged in successfully
		if (curUser != null && curUser.getUserName().equals(principal)) {
			LOG.debug("{}: Using existing principal ({}): {}", new Object[] {
					authenticator, principal, curUser });

			// no principal found
		} else {

			LOG.info("{}: Attempting kerberos login as principal ({}) from keytab "
					+ "file ({})", new Object[] { authenticator, principal, keytab });

			// attempt static kerberos login
			UserGroupInformation.loginUserFromKeytab(principal, keytab);
			curUser = UserGroupInformation.getLoginUser();
		}

		return curUser;
	}

	private void reauthenticate(String proxyUser) {
		try {
			Thread.sleep(reauthenticationRetryInterval);
		} catch (InterruptedException e) {
			// do nothing.
		}

		synchronized (lock) {
			UGIState state = proxyUserMap.get(proxyUser);
			if (state == null
					|| System.currentTimeMillis() - state.lastAuthenticated < reauthenticationRetryInterval) {
				authenticate(proxyUser);

			}
		}
	}

	/**
	 * Allow methods to act as another user (typically used for HDFS Kerberos)
	 * 
	 * @param <T>
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
				LOG.error(
						"Caught IO exception while performing a privileged action.  Reauthenticating.",
						e);
				reauthenticate(proxyUser);
				throw e;
			} catch (InterruptedException e) {
				LOG.error(
						"Caught interrupted exception while performing a privileged action.  Reauthenticating.",
						e);
				reauthenticate(proxyUser);
				throw e;
			}
		} else {
			try {
				return action.run();
			} catch (IOException ex) {
				throw ex;
			} catch (InterruptedException ex) {
				throw ex;
			} catch (RuntimeException ex) {
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
