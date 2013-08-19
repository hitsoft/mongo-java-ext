package com.hitsoft.mongo.service.migration;

import com.hitsoft.mongo.managed.ManagedService;
import com.hitsoft.mongo.repository.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * User: smeagol
 * Date: 26.12.10
 * Time: 23:19
 */
public class Migrations extends ManagedService {

  public static final Logger LOGGER = LoggerFactory.getLogger(Migrations.class);

  public static class PatchBrokenException extends Exception {
  }

  private static String parseVersion(String version) {
    return version.substring("patch_".length());
  }

  public static void migrate() throws PatchBrokenException {
    DbVersion dbVersion = DbVersion.Query.all().fetchFirst();
    if (dbVersion == null)
      dbVersion = Managed.insert(new DbVersion());
    String ver = dbVersion.version;
    if (ver == null)
      ver = "000000000000";
    for (String version : patches.keySet()) {
      if (ver.compareTo(version) < 0) {
        try {
          LOGGER.debug(String.format(">>> applying database patch: %s", version));
          Patch patch = patches.get(version).newInstance();
          patch.apply(LOGGER);
          dbVersion.update().setVersion(version);
          LOGGER.debug(String.format(">>> patch %s complete", version));
        } catch (Throwable e) {
          LOGGER.error("error: ", e);
          throw new PatchBrokenException();
        }
      }
    }
  }

  private static SortedMap<String, Class<? extends Patch>> patches = new TreeMap<String, Class<? extends Patch>>();

  public static abstract class Patch {
    public abstract void apply(Logger LOG);
  }

  private static final Pattern PATCH_CLASS_EXPR = Pattern.compile("^Patch_\\d{12}$");

  public static void registerPatch(Class<? extends Patch> clazz) {
    assert PATCH_CLASS_EXPR.matcher(clazz.getSimpleName()).matches() : "Patch class name must match pattern 'Patch_YYYYMMDDHHmm', but it does not: '" + clazz.getSimpleName() + "'";
    patches.put(parseVersion(clazz.getSimpleName()), clazz);
  }
}
