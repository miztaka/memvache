package net.vvakame.memvache;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import jp.honestyworks.pbcache.CacheContext;
import jp.honestyworks.pbcache.CacheService;
import jp.honestyworks.pbcache.QueryCache;

import com.google.apphosting.api.DatastorePb.DeleteRequest;
import com.google.apphosting.api.DatastorePb.PutRequest;
import com.google.storage.onestore.v3.OnestoreEntity.EntityProto;
import com.google.storage.onestore.v3.OnestoreEntity.Path;
import com.google.storage.onestore.v3.OnestoreEntity.Path.Element;
import com.google.storage.onestore.v3.OnestoreEntity.Reference;

/**
 * クエリーキャッシュの生存期間を示すResetDateを更新するStrategyです。
 * キャッシュの取得自体はDaoにて実装。
 * 
 * @author vvakame
 */
public class ResetDateStrategy extends RpcVisitor {
	
	static final Logger logger = Logger.getLogger(ResetDateStrategy.class.getName());

	static final int PRIORITY = 1000;

	@Override
	public int getPriority() {
		return PRIORITY;
	}

	final static Settings settings = Settings.getInstance();

	/**
	 * DatastoreにPutされたKindについて、Queryのキャッシュを参照不可にする。
	 * @param requestPb
	 * @return 常に null
	 * @author vvakame
	 */
	@Override
	public Pair<byte[], byte[]> pre_datastore_v3_Put(PutRequest requestPb) {
		
		removeCacheByEntity(requestPb.mutableEntitys());
		return null;
	}

	/**
	 * DatastoreにDeleteされたKindについて、Queryのキャッシュを参照不可にする。
	 * @param requestPb
	 * @return 常に null
	 * @author vvakame
	 */
	@Override
	public Pair<byte[], byte[]> pre_datastore_v3_Delete(DeleteRequest requestPb) {
		
		removeCache(requestPb.keys());
		return null;
	}
	
    /**
     * Remove query cache.
     * @param keys
     */
    private void removeCache(List<Reference> keys) {
        
        Map<String,Integer> buf = new HashMap<String,Integer>();
        for (Reference key: keys) {
        	String kind = getKindFromKey(key);
            if (! isIgnoreKind(kind)) {
                buf.put(kind, 1);
            }
        }
        for (String kind : buf.keySet()) {
            logger.fine("remove queryCache: " + kind);
            getQueryCache().removeQueries(kind);
        }
        return;
    }
    
    /**
     * @see removeCache
     * @param entitys
     */
    private void removeCacheByEntity(List<EntityProto> entitys) {
        
        List<Reference> keys = new ArrayList<Reference>(entitys.size());
        for (EntityProto each: entitys) {
            keys.add(each.getKey());
        }
        removeCache(keys);
        return;
    }
    
    /**
     * キーからKindを取得
     * @param key
     * @return
     */
    private String getKindFromKey(Reference key) {
        Path p = key.getPath();
        logger.finest("key: " + p);
        int size = p.elementSize();
        Element target = p.getElement(size-1);
        String kind = target.getType();
        logger.finest("kind: " + target.getType());
        return kind;
    }

    private QueryCache getQueryCache() {
    	return CacheContext.getInstance().getQueryCache();
    }

	/**
	 * 指定されたKindが予約済またはKindlessQueryまたは除外指定のKindかどうかを調べて返す。
	 * @param kind 調べるKind
	 * @return 処理対象外か否か
	 * @author vvakame
	 */
	public static boolean isIgnoreKind(String kind) {
		if (kind.startsWith("__")) {
			return true;
		} else if ("".equals(kind)) {
			return true;
		} else if (settings.getIgnoreKinds().contains(kind)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * ユーザが行うMemvacheの設定を読み取る。<br>
	 * 主に、 {@link ResetDateStrategy} に影響をおよぼす。
	 * @author vvakame
	 */
	static class Settings {

		static final Logger logger = Logger.getLogger(Settings.class.getName());

		/** 超積極的にクエリをキャッシュした時のMemcache保持秒数 */
		int expireSecond = 300;

		/** Queryをキャッシュ"しない"Kindの一覧 */
		Set<String> ignoreKinds = new HashSet<String>();

		static Settings singleton;


		/**
		 * インスタンスを取得する。
		 * @return インスタンス
		 * @author vvakame
		 */
		public static Settings getInstance() {
			if (singleton == null) {
				singleton = new Settings();
			}
			return singleton;
		}

		Settings() {
			Properties properties = new Properties();
			try {
				InputStream is = Settings.class.getResourceAsStream("/memvache.properties");
				if (is == null) {
					return;
				}
				properties.load(is);

				String expireSecondStr = properties.getProperty("expireSecond");
				if (expireSecondStr != null && !"".equals(expireSecondStr)) {
					expireSecond = Integer.parseInt(expireSecondStr);
				}

				String ignoreKindStr = properties.getProperty("ignoreKind");
				if (ignoreKindStr != null && !"".equals(ignoreKindStr)) {
					ignoreKinds = new HashSet<String>(Arrays.asList(ignoreKindStr.split(",")));
				} else {
					ignoreKinds = new HashSet<String>();
				}
				ignoreKinds.add("_ah_SESSION");
				ignoreKinds.add(CacheService.RESET_DATE_KIND);
			} catch (IOException e) {
				logger.log(Level.SEVERE, "cannot load memvache.properties", e);
			}
		}

		/**
		 * @return the expireSecond
		 * @category accessor
		 */
		public int getExpireSecond() {
			return expireSecond;
		}

		/**
		 * @param expireSecond the expireSecond to set
		 * @category accessor
		 */
		public void setExpireSecond(int expireSecond) {
			this.expireSecond = expireSecond;
		}

		/**
		 * @return the ignoreKinds
		 * @category accessor
		 */
		public Set<String> getIgnoreKinds() {
			return ignoreKinds;
		}

		/**
		 * @param ignoreKinds the ignoreKinds to set
		 * @category accessor
		 */
		public void setIgnoreKinds(Set<String> ignoreKinds) {
			this.ignoreKinds = ignoreKinds;
		}
	}
}
