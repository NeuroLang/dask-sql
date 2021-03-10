package com.dask.sql.application;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.dask.sql.schema.DaskSchema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterMultiJoinMergeRule;
import org.apache.calcite.rel.rules.FilterRemoveIsNotDistinctFromRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.MultiJoinOptimizeBushyRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.ValidationException;

/**
 * The core of the calcite program: the generator for the relational algebra.
 * Using a passed schema, it generates (optimized) relational algebra out of SQL
 * query strings or throws an exception.
 *
 * This class is taken (in parts) from the blazingSQL project.
 */
public class RelationalAlgebraGenerator {
	public enum HepExecutionType {
        SEQUENCE,
		COLLECTION
    }

	/// The created planner
	private Planner planner;
	/// The planner for optimized queries
	private HepPlanner hepPlanner;

	/// Create a new relational algebra generator from a schema
	public RelationalAlgebraGenerator(final DaskSchema schema) throws ClassNotFoundException, SQLException {
		// Taken from https://calcite.apache.org/docs/ and blazingSQL

		final CalciteConnection calciteConnection = getCalciteConnection();
		calciteConnection.setSchema(schema.getName());

		final SchemaPlus rootSchema = calciteConnection.getRootSchema();
		rootSchema.add(schema.getName(), schema);

		final FrameworkConfig config = getConfig(rootSchema, schema.getName());

		planner = Frameworks.getPlanner(config);
		hepPlanner = getHepPlanner(config);
	}

	/// Create the framework config, e.g. containing with SQL dialect we speak
	private FrameworkConfig getConfig(final SchemaPlus rootSchema, final String schemaName) {
		final List<String> defaultSchema = new ArrayList<String>();
		defaultSchema.add(schemaName);

		final Properties props = new Properties();
		props.setProperty("defaultSchema", schemaName);

		final SchemaPlus schemaPlus = rootSchema.getSubSchema(schemaName);
		final CalciteSchema calciteSchema = CalciteSchema.from(schemaPlus);

		final CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(calciteSchema, defaultSchema,
				new JavaTypeFactoryImpl(DaskSqlDialect.DASKSQL_TYPE_SYSTEM), new CalciteConnectionConfigImpl(props));

		final List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
		sqlOperatorTables.add(SqlStdOperatorTable.instance());
		sqlOperatorTables.add(SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(SqlLibrary.POSTGRESQL));
		sqlOperatorTables.add(calciteCatalogReader);

		SqlParser.Config parserConfig = getDialect().configureParser(SqlParser.Config.DEFAULT)
				.withConformance(SqlConformanceEnum.DEFAULT).withParserFactory(new DaskSqlParserImplFactory());

		SqlOperatorTable operatorTable = SqlOperatorTables.chain(sqlOperatorTables);

		// Use our defined type system
		Context defaultContext = Contexts.of(CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.TYPE_SYSTEM,
				"com.dask.sql.application.DaskSqlDialect#DASKSQL_TYPE_SYSTEM"));

		return Frameworks.newConfigBuilder().context(defaultContext).defaultSchema(schemaPlus)
				.parserConfig(parserConfig).executor(new RexExecutorImpl(null)).operatorTable(operatorTable).build();
	}

	/// Return the default dialect used
	public SqlDialect getDialect() {
		return DaskSqlDialect.DEFAULT;
	}

	/// Get a connection to "connect" to the database.
	private CalciteConnection getCalciteConnection() throws SQLException {
		// Taken from https://calcite.apache.org/docs/
		final Properties info = new Properties();
		info.setProperty("lex", "JAVA");

		final Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
		return connection.unwrap(CalciteConnection.class);
	}

	/// get an optimizer hep planner
	private HepPlanner getHepPlanner(final FrameworkConfig config) {
		final HepProgramBuilder builder = new HepProgramBuilder();
		builder.addMatchOrder(HepMatchOrder.ARBITRARY).addMatchLimit(Integer.MAX_VALUE);
		// for (RelOptRule rule : DaskRuleSets.DASK_DEFAULT_CORE_RULES){
		// 	builder.addRuleInstance(rule);
		// }

		// project rules
		builder.addSubprogram(getHepProgram(DaskRuleSets.AGGREGATE_RULES, HepMatchOrder.BOTTOM_UP, HepExecutionType.COLLECTION));
		builder.addSubprogram(getHepProgram(DaskRuleSets.PROJECT_RULES, HepMatchOrder.BOTTOM_UP, HepExecutionType.COLLECTION));
		builder.addSubprogram(getHepProgram(DaskRuleSets.FILTER_RULES, HepMatchOrder.BOTTOM_UP, HepExecutionType.COLLECTION));
		builder.addSubprogram(getHepProgram(DaskRuleSets.REDUCE_EXPRESSION_RULES, HepMatchOrder.BOTTOM_UP, HepExecutionType.COLLECTION));
		// join reorder
		builder.addSubprogram(getHepProgram(DaskRuleSets.JOIN_REORDER_PREPARE_RULES, HepMatchOrder.BOTTOM_UP, HepExecutionType.COLLECTION));
		builder.addSubprogram(getHepProgram(DaskRuleSets.JOIN_REORDER_RULES, HepMatchOrder.BOTTOM_UP, HepExecutionType.SEQUENCE));

		// project rules
		builder.addSubprogram(getHepProgram(DaskRuleSets.PROJECT_RULES, HepMatchOrder.BOTTOM_UP, HepExecutionType.COLLECTION));
		// optimize logical plan
		builder.addSubprogram(getHepProgram(DaskRuleSets.LOGICAL_RULES, HepMatchOrder.BOTTOM_UP, HepExecutionType.SEQUENCE));

		return new HepPlanner(builder.build(), config.getContext());
	}

	/**
	 * Builds a HepProgram for the given set of rules and with the given order. If type is COLLECTION,
	 * rules are added as collection. Otherwise, rules are added sequentially.
	 * @param rules
	 * @param order
	 * @param type
	 * @return
	 */
	private HepProgram getHepProgram(final RuleSet rules, final HepMatchOrder order, final HepExecutionType type) {
		final HepProgramBuilder builder = new HepProgramBuilder().addMatchOrder(order);
		switch (type) {
            case SEQUENCE:
				for (RelOptRule rule : rules) {
					builder.addRuleInstance(rule);
				}
                break;
			case COLLECTION:
				List<RelOptRule> rulesCollection = new ArrayList<RelOptRule>();
				rules.iterator().forEachRemaining(rulesCollection::add);
				builder.addRuleCollection(rulesCollection);
				break;
        }
		return builder.build();
	}

	/// Parse a sql string into a sql tree
	public SqlNode getSqlNode(final String sql) throws SqlParseException {
		try {
			return planner.parse(sql);
		} catch (final SqlParseException e) {
			planner.close();
			throw e;
		}
	}

	/// Validate a sql node
	public SqlNode getValidatedNode(final SqlNode sqlNode) throws ValidationException {
		try {
			return planner.validate(sqlNode);
		} catch (final ValidationException e) {
			planner.close();
			throw e;
		}
	}

	/// Turn a validated sql node into a rel node
	public RelNode getRelationalAlgebra(final SqlNode validatedSqlNode) throws RelConversionException {
		try {
			return planner.rel(validatedSqlNode).project(true);
		} catch (final RelConversionException e) {
			planner.close();
			throw e;
		}
	}

	/// Turn a non-optimized algebra into an optimized one
	public RelNode getOptimizedRelationalAlgebra(final RelNode nonOptimizedPlan) {
		hepPlanner.setRoot(nonOptimizedPlan);
		planner.close();

		return hepPlanner.findBestExp();
	}

	/// Return the string representation of a rel node
	public String getRelationalAlgebraString(final RelNode relNode) {
		return RelOptUtil.toString(relNode);
	}
}

