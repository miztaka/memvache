package net.vvakame.memvache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityTranslatorPublic;
import com.google.appengine.api.datastore.Key;
import com.google.apphosting.api.DatastorePb;
import com.google.apphosting.api.DatastorePb.Cursor;
import com.google.apphosting.api.DatastorePb.NextRequest;
import com.google.apphosting.api.DatastorePb.Query;
import com.google.apphosting.api.DatastorePb.QueryResult;
import com.google.storage.onestore.v3.OnestoreEntity.EntityProto;
import com.google.storage.onestore.v3.OnestoreEntity.Reference;

/**
 * "Datastore への Query をKeysOnlyに差し替え" を実装するクラス。<br>
 * Datastore への Query をKeysOnlyに書き換え、取れたKeyに対してMemcacheに照会を実施し不足分についてBatchGetを行う戦略を実装する。
 * @author vvakame
 */
public class QueryKeysOnlyStrategy extends RpcVisitor {

	static final int PRIORITY = AggressiveQueryCacheStrategy.PRIORITY + 1000;


	@Override
	public int getPriority() {
		return PRIORITY;
	}

	List<Query> rewritedQuery = new ArrayList<Query>();

	List<Cursor> rewritedCursor = new ArrayList<Cursor>();

	/**
	 * DatastoreのQueryについて、KeysOnlyがfalseの場合はtrueに書き換える。
	 */
	@Override
	public Pair<byte[], byte[]> pre_datastore_v3_RunQuery(Query requestPb) {
		
		logger.fine("pre_datastore_v3_RunQuery start: " + Thread.currentThread().getId() + " " + this);
		logger.fine("kind: " + requestPb.getKind());
		
		if (requestPb.isKeysOnly()) {
			logger.fine("this query is originally KeysOnly. nothing to do.");
			return null;
		}
		if (requestPb.getKind().startsWith("__")) {
			return null;
		}

		requestPb.setKeysOnly(true);
		rewritedQuery.add(requestPb);
		
		logger.fine("rerwite query to keys only: " + requestPb.getKind());
		return Pair.request(requestPb.toByteArray());
	}

	/**
	 * もし、preでKeysOnlyをtrueに書き換えていた場合、取得できたKeyを元にBatchGetを行う。<br>
	 * BatchGetの結果を元にKeysOnlyではない、普通のクエリの結果のように肉付けしてやる。<br>
	 * BatchGetを行う時に、Memcacheから既知のEntityを取得する作業は {@link GetPutCacheStrategy} が行なってくれる。
	 */
	@Override
	public byte[] post_datastore_v3_RunQuery(Query requestPb, QueryResult responsePb) {
		
		logger.fine("post_datastore_v3_RunQuery start: " + Thread.currentThread().getId() + " " + this);
		
		if (!rewritedQuery.contains(requestPb)) {
			logger.fine("not rewritedQuery: do nothing.");
			return null;
		}

		// Nextのためにカーソルを覚えておく
		if (responsePb.isMoreResults()) {
			rewritedCursor.add(responsePb.getCursor());
			logger.fine("list up cursor: " + responsePb.getCursor());
		}

		reconstructQueryResult(responsePb);
		logger.fine("Query Result reconstructed using cache.");

		// TODO compiledQuery, compiledCursor, cursor, index, indexOnly …etcについてKeysOnlyにしたことで挙動が変わるかを調査しないとアカン。
		// TODO RunCompiledQuery, Next のmethodについても調査が必要かなぁ…

		return responsePb.toByteArray();
	}

	/**
	 * RunQueryでkeysOnlyに書き換えたものについてはNextの実行結果も肉付けする。
	 * 
	 * @param requestPb
	 * @param responsePb 
	 * @return 処理の返り値 or null
	 * @author vvakame
	 */
	@Override
	public byte[] post_datastore_v3_Next(NextRequest requestPb, QueryResult responsePb) {
		
		logger.fine("post_datastore_v3_Next start: " + Thread.currentThread().getId() + " " + this);
		logger.fine("cursor: " + requestPb.getCursor());

		if (rewritedCursor == null || !rewritedCursor.contains(requestPb.getCursor())) {
			logger.fine("no rewrited cursor found.");
			return null;
		}
		logger.fine("Next: rewrited cursor.");

		reconstructQueryResult(responsePb);

		return responsePb.toByteArray();
	}

	/**
	 * keysOnlyのQueryResultに肉付けをする処理
	 * Memcache呼び出しはGetPutStrategyに任せる
	 * 
	 * @param responsePb
	 */
	void reconstructQueryResult(QueryResult responsePb) {

		// 検索結果(KeysOnly)
		List<Key> keys;
		{
			List<EntityProto> protos = responsePb.results();
			List<Reference> requestedKeys = new ArrayList<Reference>();
			for (EntityProto proto : protos) {
				requestedKeys.add(proto.getKey());
			}

			keys = PbKeyUtil.toKeys(requestedKeys);
			logger.info("key count: " + keys.size());
		}
		
		/*
		// EntityをBatchGet(Memcacheからの取得はGetPutCacheStrategyにお任せ)
		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
		Map<Key, Entity> batchGet = datastore.get(keys);
		logger.log(Level.FINE, "batchGet count: " + batchGet.size());
		
		// 検索結果を組み立てる
		responsePb.setKeysOnly(false);
		responsePb.clearResult();
		for (Key key : keys) {
			//Entity entityByGet = (batchGet == null) ? null : batchGet.get(key);
			Entity entityByGet = batchGet.get(key);
			if (entityByGet != null) {
				EntityProto proto = EntityTranslatorPublic.convertToPb(entityByGet);
				responsePb.addResult(proto);
			}
		}
		*/

		// MemcacheからEntity部分を取得
		Map<Key, DatastorePb.GetResponse.Entity> cached;
		{
			Map<Key, Object> all = MemvacheDelegate.getMemcache().getAll(keys);
			if (all == null) {
				logger.severe("memcache#getAll returns null. continue.");
				all = new LinkedHashMap<Key,Object>();
			}
			cached = MemcacheKeyUtil.conv(all);
			logger.info("STAT:BatchGetKeysOnly,hit," + cached.size());
		}

		// Memcacheから取得できなかった部分をBatchGet
		Map<Key, Entity> batchGet = null;
		if (cached.size() != keys.size()) {
			List<Key> missingKeys = new ArrayList<Key>();
			for (Key key : keys) {
				if (!cached.containsKey(key)) {
					missingKeys.add(key);
				}
			}

			DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
			batchGet = datastore.get(missingKeys);
			logger.info("STAT:BatchGetKeysOnly,miss," + batchGet.size());
		}

		// 1つの検索結果であるかのように組み立てる
		responsePb.setKeysOnly(false);
		responsePb.clearResult();
		
		for (Key key : keys) {
			Entity entityByGet = (batchGet == null) ? null : batchGet.get(key);
			if (entityByGet != null) {
				EntityProto proto = EntityTranslatorPublic.convertToPb(entityByGet);
				responsePb.addResult(proto);
			} else {
				DatastorePb.GetResponse.Entity entity = cached.get(key);
				if (entity != null) {
					responsePb.addResult(entity.getEntity());
				} else {
					logger.severe("Missing entity: " + key);
				}
			}
		}
	}
}
