package com.github.slavaz.maven.plugin.postgresql.embedded.psql;

import com.github.slavaz.maven.plugin.postgresql.embedded.psql.util.PostgresConfigUtil;
import org.apache.maven.plugin.MojoExecutionException;
import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;

/**
 * Created by slavaz on 13/02/17.
 */
public class PgInstanceManager {

    private static PostgresProcess process = null;

    public static void start(final IPgInstanceProcessData pgInstanceProcessData) throws IOException {
        if (process != null) {
            throw new IllegalStateException("Postgres already started");
        }

        final PostgresStarter<PostgresExecutable, PostgresProcess> postgresStarter =
                PostgresStarter.getDefaultInstance();

        final PostgresConfig postgresConfig = PostgresConfigUtil.get(pgInstanceProcessData);

        PostgresExecutable postgresExecutable = postgresStarter.prepare(postgresConfig);

        process = postgresExecutable.start();

        // restore dump?
        if(Objects.nonNull(pgInstanceProcessData.getRestoreFile())){
            process.restoreFromFile(getFile(pgInstanceProcessData.getRestoreFile()));
        }
        // import SQL?
        if(Objects.nonNull(pgInstanceProcessData.getImportFile())){
            process.importFromFile(getFile(pgInstanceProcessData.getImportFile()));
        }
    }

    private static File getFile(String path) {
        File f = new File(path);
        if(!f.exists()){
            throw new RuntimeException("File does not exist: " + path);
        }
        return f;
    }

    public static void stop() throws InterruptedException {
        if (process != null) {
            PostgresProcess p = process;
            process = null;
            p.stop();
            p.waitFor();
        }
    }

}
