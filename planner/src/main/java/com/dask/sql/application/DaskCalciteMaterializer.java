package com.dask.sql.application;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import com.dask.sql.schema.DaskTable;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;

/**
 * Utility class for preparing a list of RelOptMaterializations which can then
 * be added to a RelOptPlanner (hep or volcano) to be used during optimization.
 * 
 * Create the class using the constructor with the rootSchema, defaultSchemaName,
 * and FrameworkConfig used to create a Planner. Then call getMaterializations()
 * to get the list of RelOptMaterializations, containing a RelOptMaterialization
 * for each view in the schema. A view is any DaskTable which has an sql query
 * associated.
 * 
 * A lot of the code for this class is taken / adapted from :
 * https://github.com/apache/calcite/blob/master/core/src/main/java/org/apache/calcite/prepare/Prepare.java
 * https://github.com/apache/calcite/blob/master/core/src/main/java/org/apache/calcite/prepare/CalciteMaterializer.java
 * https://github.com/apache/calcite/blob/master/core/src/main/java/org/apache/calcite/prepare/CalcitePrepareImpl.java
 * 
 */
public class DaskCalciteMaterializer {

    private final CatalogReader catalogReader;
    private final CalciteSchema schema;
    private final SqlValidator sqlValidator;
    private final JavaTypeFactory typeFactory;
    private final FrameworkConfig config;
    private final PlannerImpl planner;

    DaskCalciteMaterializer(final SchemaPlus rootSchema, final String schemaName, final FrameworkConfig config) {
        final SchemaPlus schemaPlus = rootSchema.getSubSchema(schemaName);
        schema = CalciteSchema.from(schemaPlus);
        this.config = config;
        planner = new PlannerImpl(config);

        final List<String> schemaPath = new ArrayList<String>();
        schemaPath.add(schema.getName());
        final Properties props = new Properties();
        props.setProperty("defaultSchema", schema.getName());
        catalogReader = new CalciteCatalogReader(schema.root(), schemaPath,
                new JavaTypeFactoryImpl(DaskSqlDialect.DASKSQL_TYPE_SYSTEM), new CalciteConnectionConfigImpl(props));

        final List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
        sqlOperatorTables.add(SqlStdOperatorTable.instance());
        sqlOperatorTables.add(SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(SqlLibrary.POSTGRESQL));
        sqlOperatorTables.add(catalogReader);
        SqlOperatorTable operatorTable = SqlOperatorTables.chain(sqlOperatorTables);

        typeFactory = new JavaTypeFactoryImpl(DaskSqlDialect.DASKSQL_TYPE_SYSTEM);
        final CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(props);
        final SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                .withSqlConformance(connectionConfig.conformance())
                .withDefaultNullCollation(connectionConfig.defaultNullCollation()).withIdentifierExpansion(true);

        sqlValidator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, validatorConfig);
    }

    /**
     * Prepare a list of RelOptMaterialization to be added to the planner before optimizing
     */
    public List<RelOptMaterialization> getMaterializations() {
        List<RelOptMaterialization> materializations = new ArrayList<RelOptMaterialization>();
        for (String tableName : schema.getTableNames()) {
            CalciteSchema.TableEntry tableEntry = schema.getTable(tableName, true);
            DaskTable table = (DaskTable) tableEntry.getTable();
            if (table.isMaterializedView()) {
                List<String> qualifiedTableName = tableEntry.path();
                // Create a materialization with the Table and SQL query
                final Materialization materialization = new Materialization(tableEntry, table.getSql(), qualifiedTableName);
                // Populate this materialization's tableRel and queryRel with the corresponding 
                // RelNode representation for the query and table
                populate(materialization);
                // Create a RelOptMaterialization to add to the list of materializations
                materializations.add(
                new RelOptMaterialization(
                    materialization.tableRel,
                    materialization.queryRel,
                    materialization.starRelOptTable,
                    qualifiedTableName));
            }
        }
        return materializations;
    }

    protected SqlToRelConverter getSqlToRelConverter(SqlValidator validator, CatalogReader catalogReader,
            SqlToRelConverter.Config relConfig) {
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);
        final RelOptPlanner optPlanner = new VolcanoPlanner(this.config.getCostFactory(), this.config.getContext());
        final RelOptCluster cluster = RelOptCluster.create(optPlanner, rexBuilder);
        return new SqlToRelConverter(planner, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE,
                relConfig);
    }

    /**
     * Populates a materialization record, converting an sql query string and
     * table path (essentially a list of strings, like ["hr", "sales"]) into 
     * RelNodes which can be used during the relational algebra planning process.
     */
    protected void populate(final Materialization materialization) {
        SqlParser parser = SqlParser.create(materialization.sql, config.getParserConfig());
        SqlNode node;
        try {
            node = parser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException("parse failed", e);
        }
        final SqlToRelConverter.Config relConfig = SqlToRelConverter.config().withTrimUnusedFields(true);
        SqlToRelConverter sqlToRelConverter2 = getSqlToRelConverter(sqlValidator, catalogReader, relConfig);

        RelRoot root = sqlToRelConverter2.convertQuery(node, true, true);
        materialization.queryRel = trimUnusedFields(root).rel;

        // Identify and substitute a StarTable in queryRel.
        //
        // It is possible that no StarTables match. That is OK, but the
        // materialization patterns that are recognized will not be as rich.
        //
        // It is possible that more than one StarTable matches. TBD: should we
        // take the best (whatever that means), or all of them?
        useStar(schema, materialization);

        List<String> tableName = materialization.materializedTable.path();
        RelOptTable table = this.catalogReader.getTable(tableName);
        materialization.tableRel = sqlToRelConverter2.toRel(table, ImmutableList.of());
    }

    /**
     * Walks over a tree of relational expressions, replacing each
     * {@link org.apache.calcite.rel.RelNode} with a 'slimmed down' relational
     * expression that projects only the columns required by its consumer.
     *
     * @param root Root of relational expression tree
     * @return Trimmed relational expression
     */
    protected RelRoot trimUnusedFields(RelRoot root) {
        final SqlToRelConverter.Config config = SqlToRelConverter.config().withTrimUnusedFields(shouldTrim(root.rel))
                .withExpand(false);
        final SqlToRelConverter converter = getSqlToRelConverter(sqlValidator, catalogReader, config);
        final boolean ordered = !root.collation.getFieldCollations().isEmpty();
        final boolean dml = SqlKind.DML.contains(root.kind);
        return root.withRel(converter.trimUnusedFields(dml || ordered, root.rel));
    }

    private static boolean shouldTrim(RelNode rootRel) {
        // For now, don't trim if there are more than 3 joins. The projects
        // near the leaves created by trim migrate past joins and seem to
        // prevent join-reordering.
        return RelOptUtil.countJoins(rootRel) < 2;
    }

    /**
     * Converts a relational expression to use a {@link StarTable} defined in
     * {@code schema}. Uses the first star table that fits.
     */
    private void useStar(CalciteSchema schema, Materialization materialization) {
        RelNode queryRel = materialization.queryRel;
        for (Callback x : useStar(schema, queryRel)) {
            // Success -- we found a star table that matches.
            materialization.materialize(x.rel, x.starRelOptTable);
            System.out.println("Materialization " + materialization.materializedTable + " matched star table "
                    + x.starTable + "; query after re-write: " + RelOptUtil.toString(queryRel));
        }
    }

    /**
     * Converts a relational expression to use a
     * {@link org.apache.calcite.schema.impl.StarTable} defined in {@code schema}.
     * Uses the first star table that fits.
     */
    private Iterable<Callback> useStar(CalciteSchema schema, RelNode queryRel) {
        List<CalciteSchema.TableEntry> starTables = Schemas.getStarTables(schema.root());
        if (starTables.isEmpty()) {
            // Don't waste effort converting to leaf-join form.
            return ImmutableList.of();
        }
        final List<Callback> list = new ArrayList<>();
        final RelNode rel2 = RelOptMaterialization.toLeafJoinForm(queryRel);
        for (CalciteSchema.TableEntry starTable : starTables) {
            final Table table = starTable.getTable();
            assert table instanceof StarTable;
            RelOptTableImpl starRelOptTable = RelOptTableImpl.create(catalogReader, table.getRowType(typeFactory),
                    starTable, null);
            final RelNode rel3 = RelOptMaterialization.tryUseStar(rel2, starRelOptTable);
            if (rel3 != null) {
                list.add(new Callback(rel3, starTable, starRelOptTable));
            }
        }
        return list;
    }

    /** Called when we discover a star table that matches. */
    static class Callback {
        public final RelNode rel;
        public final CalciteSchema.TableEntry starTable;
        public final RelOptTableImpl starRelOptTable;

        Callback(RelNode rel, CalciteSchema.TableEntry starTable, RelOptTableImpl starRelOptTable) {
            this.rel = rel;
            this.starTable = starTable;
            this.starRelOptTable = starRelOptTable;
        }
    }

    /**
     * Describes that a given SQL query is materialized by a given table. The
     * materialization is currently valid, and can be used in the planning process.
     */
    public static class Materialization {
        /** The table that holds the materialized data. */
        final CalciteSchema.TableEntry materializedTable;
        /** The query that derives the data. */
        final String sql;
        /** The schema path for the query. */
        final List<String> viewSchemaPath;
        /**
         * Relational expression for the table. Usually a
         * {@link org.apache.calcite.rel.logical.LogicalTableScan}.
         */
        @Nullable
        RelNode tableRel;
        /** Relational expression for the query to populate the table. */
        @Nullable
        RelNode queryRel;
        /** Star table identified. */
        private @Nullable RelOptTable starRelOptTable;

        public Materialization(CalciteSchema.TableEntry materializedTable, String sql, List<String> viewSchemaPath) {
            assert materializedTable != null;
            assert sql != null;
            this.materializedTable = materializedTable;
            this.sql = sql;
            this.viewSchemaPath = viewSchemaPath;
        }

        public void materialize(RelNode queryRel, RelOptTable starRelOptTable) {
            this.queryRel = queryRel;
            this.starRelOptTable = starRelOptTable;
            // assert starRelOptTable.maybeUnwrap(StarTable.class).isPresent();
        }
    }
}
