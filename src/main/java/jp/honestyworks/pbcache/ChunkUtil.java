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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ChunkUtil {

	public static List<byte[]> makeChunks(byte[] data, int chunkSize) {
		List<byte[]> result = new ArrayList<byte[]>();
		long n = data.length / chunkSize;
		int finalChunkSize = data.length % chunkSize;
		int i = 0;
		int start;
		int end;
		for (i = 0; i < n; i++) {
			start = i * chunkSize;
			end = start + chunkSize;
			result.add(Arrays.copyOfRange(data, start, end));
		}
		if (finalChunkSize > 0) {
			start = i * chunkSize;
			end = start + finalChunkSize;
			result.add(Arrays.copyOfRange(data, start, end));
		}
		return result;
	}
	
	public static byte[] packChunks(List<byte[]> data) {
		int size = 0;
		for (byte[] chunk : data) {
			size += chunk.length;
		}
		byte[] result = new byte[size];
		int start = 0;
		for (byte[] chunk : data) {
			System.arraycopy(chunk, 0, result, start, chunk.length);
			start += chunk.length;
		}
		return result;
	}
	
}
