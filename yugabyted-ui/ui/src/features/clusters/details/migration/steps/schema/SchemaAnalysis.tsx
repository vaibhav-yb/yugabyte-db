import React, { FC } from "react";
import { Box, makeStyles, Paper, Typography, useTheme } from "@material-ui/core";
import { BadgeVariant, YBBadge } from "@app/components/YBBadge/YBBadge";
import { useTranslation } from "react-i18next";
import { YBAccordion } from "@app/components";
import RestartIcon from "@app/assets/restart2.svg";
import { SchemaAnalysisTabs } from "./SchemaAnalysisTabs";
import type { Migration } from "../../MigrationOverview";
import type { MigrateSchemaTaskInfo, RefactoringCount, UnsupportedSqlInfo } from "@app/api/src";

export type SchemaAnalysisData = {
  completedOn?: string;
  manualRefactorObjectsCount: number | undefined;
  summary: {
    graph: RefactoringCount[];
  };
  reviewRecomm: {
    unsupportedDataTypes: UnsupportedSqlInfo[] | undefined;
    unsupportedFeatures: UnsupportedSqlInfo[] | undefined;
    unsupportedFunctions: UnsupportedSqlInfo[] | undefined;
  };
};

const useStyles = makeStyles((theme) => ({
  paper: {
    border: "1px solid",
    borderColor: theme.palette.primary[200],
    backgroundColor: theme.palette.primary[100],
    textAlign: "center",
  },
  icon: {
    marginTop: theme.spacing(1),
    flexShrink: 0,
    height: "fit-content",
  },
  badge: {
    height: "32px",
    width: "32px",
    borderRadius: "100%",
  },
  accordionHeader: {
    flex: 1,
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: theme.spacing(1),
  },
  completedTime: {
    color: theme.palette.grey[700],
    fontSize: "0.7rem",
  },
}));

interface SchemaAnalysisProps {
  migration: Migration | undefined;
  schemaAPI: MigrateSchemaTaskInfo;
}

export const SchemaAnalysis: FC<SchemaAnalysisProps> = ({ /* migration, */ schemaAPI }) => {
  const classes = useStyles();
  const theme = useTheme();
  const { t } = useTranslation();

  const currentAnalysisReport = schemaAPI.current_analysis_report;
  const history = schemaAPI?.analysis_history ?? [];

  const analysis: SchemaAnalysisData[] = [currentAnalysisReport, ...history].filter(Boolean)
    .map((analysisReport) => ({
      completedOn: "",
      manualRefactorObjectsCount: analysisReport?.recommended_refactoring?.refactor_details
        ?.reduce((acc, { manual }) => acc + (manual ?? 0), 0) || 0,
      summary: {
        graph: analysisReport?.recommended_refactoring?.refactor_details ?? [],
      },
      reviewRecomm: {
        unsupportedDataTypes: analysisReport?.unsupported_datatypes ?? [],
        unsupportedFeatures: analysisReport?.unsupported_features ?? [],
        unsupportedFunctions: analysisReport?.unsupported_functions ?? [],
      },
    }));

  return (
    <Box>
      <Paper className={classes.paper}>
        <Box px={2} py={1.5} display="flex" alignItems="center" gridGap={theme.spacing(2)}>
          <YBBadge
            className={classes.badge}
            text=""
            variant={BadgeVariant.InProgress}
            iconComponent={RestartIcon}
          />
          <Typography variant="body2" align="left">
            {t("clusterDetail.voyager.migrateSchema.rerunAnalysis")}
          </Typography>
        </Box>
      </Paper>

      <Box display="flex" flexDirection="column" gridGap={theme.spacing(2)} my={2}>
        {analysis.map((item, index) => (
          <YBAccordion
            key={index}
            titleContent={
              <Typography variant="body2" className={classes.accordionHeader}>
                {t("clusterDetail.voyager.migrateSchema.analysis")}
                <Box display="flex" alignItems="center" gridGap={theme.spacing(1)}>
                  {item.completedOn && (
                    <Typography variant="body2" className={classes.completedTime}>
                      {item.completedOn}
                    </Typography>
                  )}
                  {item.manualRefactorObjectsCount != null && (
                    <YBBadge
                      text={t("clusterDetail.voyager.migrateSchema.objectsToRefactorManually", {
                        count: item.manualRefactorObjectsCount,
                      })}
                      variant={
                        item.manualRefactorObjectsCount === 0
                          ? BadgeVariant.Success
                          : BadgeVariant.Warning
                      }
                    />
                  )}
                </Box>
              </Typography>
            }
            defaultExpanded={index === 0}
            contentSeparator
          >
            <SchemaAnalysisTabs analysis={item} />
          </YBAccordion>
        ))}
      </Box>
    </Box>
  );
};
