/*
 * Copyright 2012 Honestyworks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package jp.honestyworks.pbcache;

import java.io.Serializable;
import java.util.Date;

/**
 * 
 * @author miztaka
 *
 */
@SuppressWarnings("serial")
public class CacheItem implements Serializable {

	private Object data;
	private Date timestamp;
	
	public Object getData() {
		return data;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public CacheItem(Object data) {
		super();
		this.data = data;
		this.timestamp = new Date();
	}

}
