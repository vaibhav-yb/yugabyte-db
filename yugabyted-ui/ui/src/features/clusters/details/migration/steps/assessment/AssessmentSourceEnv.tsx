import React, { FC } from "react";
import { Box, Grid, Paper, Typography, makeStyles, useTheme } from "@material-ui/core";
import { useTranslation } from "react-i18next";
import { MigrationSourceEnvSidePanel } from "./AssessmentSourceEnvSidePanel";
import { getMemorySizeUnits } from "@app/helpers";
import type { Migration } from "../../MigrationOverview";
import { YBButton } from "@app/components";
import CaretRightIcon from "@app/assets/caret-right-circle.svg";

const useStyles = makeStyles((theme) => ({
  heading: {
    marginBottom: theme.spacing(4),
  },
  label: {
    color: theme.palette.grey[500],
    fontWeight: theme.typography.fontWeightMedium as number,
    marginBottom: theme.spacing(0.75),
    textTransform: "uppercase",
    textAlign: "left",
  },
  dividerHorizontal: {
    width: "100%",
    marginTop: theme.spacing(2.5),
    marginBottom: theme.spacing(2.5),
  },
  value: {
    paddingTop: theme.spacing(0.36),
    textAlign: "start",
  },
  pointer: {
    cursor: "pointer",
  },
  dividerVertical: {
    marginLeft: theme.spacing(2.5),
    marginRight: theme.spacing(2.5),
  },
}));

interface MigrationSourceEnvProps {
  vcpu: string;
  memory: string;
  disk: string;
  connectionCount: string;
  tableSize: string | number;
  indexSize: string | number;
  totalSize: string | number;
  rowCount: string;
  migration: Migration | undefined;
}

export const MigrationSourceEnv: FC<MigrationSourceEnvProps> = ({
  /* vcpu,
  memory,
  disk,
  connectionCount, */
  tableSize,
  indexSize,
  totalSize,
  /* rowCount, */
  migration,
}) => {
  const classes = useStyles();
  const { t } = useTranslation();
  const theme = useTheme();

  const [showSourceObjects, setShowSourceObjects] = React.useState<boolean>(false);

  return (
    <Paper>
      <Box px={2} py={3}>
        <Box display="flex">
          {/* <Box flex={1}>
            <Box display="flex" alignItems="center" gridGap={theme.spacing(0.6)} mb={3}>
              <Typography variant="h5">
                {t("clusterDetail.voyager.planAndAssess.sourceEnv.heading")}
              </Typography>
              <Box>
                <YBTooltip title={t("clusterDetail.voyager.planAndAssess.sourceEnv.tooltip")} />
              </Box>
            </Box>

            <Grid container spacing={4}>
              <Grid item xs={6}>
                <Typography variant="subtitle2" className={classes.label}>
                  {t("clusterDetail.voyager.planAndAssess.sourceEnv.totalVcpu")}
                </Typography>
                <Typography variant="body2" className={classes.value}>
                  {vcpu}
                </Typography>
              </Grid>
              <Grid item xs={6}>
                <Typography variant="subtitle2" className={classes.label}>
                  {t("clusterDetail.voyager.planAndAssess.sourceEnv.totalMemory")}
                </Typography>
                <Typography variant="body2" className={classes.value}>
                  {memory}
                </Typography>
              </Grid>
              <Grid item xs={6}>
                <Typography variant="subtitle2" className={classes.label}>
                  {t("clusterDetail.voyager.planAndAssess.sourceEnv.totalDisk")}
                </Typography>
                <Typography variant="body2" className={classes.value}>
                  {disk}
                </Typography>
              </Grid>
              <Grid item xs={6}>
                <Typography variant="subtitle2" className={classes.label}>
                  {t("clusterDetail.voyager.planAndAssess.sourceEnv.noOfConns")}
                </Typography>
                <Typography variant="body2" className={classes.value}>
                  {connectionCount}
                </Typography>
              </Grid>
            </Grid>
          </Box>

          <Divider orientation="vertical" className={classes.dividerVertical} flexItem /> */}

          <Box flex={1}>
            <Box
              display="flex"
              alignItems="center"
              justifyContent="space-between"
              gridGap={theme.spacing(0.6)}
              mb={3}
            >
              <Typography variant="h5">
                {t("clusterDetail.voyager.planAndAssess.sourceEnv.sourceDB")}
              </Typography>
              <YBButton
                variant="ghost"
                startIcon={<CaretRightIcon />}
                onClick={() => setShowSourceObjects(true)}
              >
                {t("clusterDetail.voyager.planAndAssess.sourceEnv.viewDetails")}
              </YBButton>
            </Box>

            <Grid container spacing={4}>
              <Grid item xs={4}>
                <Typography variant="subtitle2" className={classes.label}>
                  {t("clusterDetail.voyager.planAndAssess.sourceEnv.tableSize")}
                </Typography>
                <Typography variant="body2" className={classes.value}>
                  {typeof tableSize === "number" ? getMemorySizeUnits(tableSize) : tableSize}
                </Typography>
              </Grid>
              {/* <Grid item xs={?}>
                <Typography variant="subtitle2" className={classes.label}>
                  {t("clusterDetail.voyager.planAndAssess.sourceEnv.rowCount")}
                </Typography>
                <Typography variant="body2" className={classes.value}>
                  {rowCount}
                </Typography>
              </Grid> */}

              <Grid item xs={4}>
                <Typography variant="subtitle2" className={classes.label}>
                  {t("clusterDetail.voyager.planAndAssess.sourceEnv.totalSize")}
                </Typography>
                <Typography variant="body2" className={classes.value}>
                  {typeof totalSize === "number" ? getMemorySizeUnits(totalSize) : totalSize}
                </Typography>
              </Grid>
              <Grid item xs={4}>
                <Typography variant="subtitle2" className={classes.label}>
                  {t("clusterDetail.voyager.planAndAssess.sourceEnv.indexSize")}
                </Typography>
                <Typography variant="body2" className={classes.value}>
                  {typeof indexSize === "number" ? getMemorySizeUnits(indexSize) : indexSize}
                </Typography>
              </Grid>
            </Grid>
          </Box>
        </Box>
      </Box>

      <MigrationSourceEnvSidePanel
        migration={migration}
        open={showSourceObjects}
        onClose={() => setShowSourceObjects(false)}
      />
    </Paper>
  );
};
