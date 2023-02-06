import React, { useContext, FC } from 'react';
import { useTranslation } from 'react-i18next';
import { useForm, FormProvider } from 'react-hook-form';
import { Typography, Grid, Box, Link } from '@material-ui/core';
import { YBButton } from '../../../../components';
import {
  AdvancedConfiguration,
  CloudConfiguration,
  GFlags,
  HelmOverrides,
  InstanceConfiguration,
  UserTags,
  UniverseResourceContainer
} from './sections';
import { UniverseFormContext } from '../UniverseFormContainer';
import { UniverseFormData, ClusterType, ClusterModes } from '../utils/dto';
import { UNIVERSE_NAME_FIELD } from '../utils/constants';
import { useFormMainStyles } from '../universeMainStyle';

// ! How to add new form field ?
// - add field to it's corresponding config type (CloudConfigFormValue/InstanceConfigFormValue/AdvancedConfigFormValue/..) present in UniverseFormData type at dto.ts
// - set default value in corresponding const (DEFAULT_CLOUD_CONFIG/DEFAULT_INSTANCE_CONFIGDEFAULT_ADVANCED_CONFIG/..) presnt in DEFAULT_FORM_DATA at dto.ts
// - populate form value from universe object in getFormData() at utils/helpers
// - Map form value to universe payload in getUserIntent() at utils/helper before submitting
// - Submit logic/flags needed for each cluster operation is written in it's own file(CreateUniverse/CreateRR/EditUniverse/EditRR)
// - Import actual form field ui component in corresponding section(cloud/instance/advanced/..)

// ! Field component rules and requirements
// - should update itself only and don't modify other form fields
// - should have custom field logic and field validation logic, if needed
// - could watch other form field values

interface UniverseFormProps {
  defaultFormData: UniverseFormData;
  onFormSubmit: (data: UniverseFormData) => void;
  onCancel: () => void;
  onClusterTypeChange?: (data: UniverseFormData) => void;
  onDeleteRR?: () => void;
  submitLabel?: string;
  isNewUniverse?: boolean; // This flag is used only in new cluster creation flow - we don't have proper state params to differentiate
}

export const UniverseForm: FC<UniverseFormProps> = ({
  defaultFormData,
  onFormSubmit,
  onCancel,
  onClusterTypeChange,
  onDeleteRR,
  submitLabel,
  isNewUniverse = false
}) => {
  const classes = useFormMainStyles();
  const { t } = useTranslation();

  //context state
  const { asyncFormData, clusterType, mode, universeResourceTemplate } = useContext(
    UniverseFormContext
  )[0];
  const isPrimary = clusterType === ClusterType.PRIMARY;
  const isEditMode = mode === ClusterModes.EDIT;
  const isEditRR = isEditMode && !isPrimary;

  //init form
  const formMethods = useForm<UniverseFormData>({
    mode: 'onChange',
    reValidateMode: 'onChange',
    defaultValues: defaultFormData,
    shouldFocusError: true
  });
  const { getValues, trigger } = formMethods;

  //methods
  const triggerValidation = () => trigger(undefined, { shouldFocus: true }); //Trigger validation and focus on fields with errors , undefined = validate all fields
  const onSubmit = (formData: UniverseFormData) => onFormSubmit(formData);
  const switchClusterType = () => onClusterTypeChange && onClusterTypeChange(getValues());

  //switching from primary to RR and vice versa  (Create Primary + RR flow)
  const handleClusterChange = async () => {
    if (isPrimary) {
      // Validate primary form before switching to async
      let isValid = await triggerValidation();
      isValid && switchClusterType();
    } else {
      //switching from async to primary
      switchClusterType();
    }
  };

  const renderHeader = () => {
    return (
      <>
        <Typography className={classes.headerFont}>
          {isNewUniverse ? t('universeForm.createUniverse') : getValues(UNIVERSE_NAME_FIELD)}
        </Typography>
        {!isNewUniverse && (
          <Typography className={classes.subHeaderFont}>
            <i className="fa fa-chevron-right"></i> &nbsp;
            {isPrimary ? t('universeForm.editUniverse') : t('universeForm.configReadReplica')}
          </Typography>
        )}
        {onClusterTypeChange && (
          <>
            <Box
              flexShrink={1}
              display={'flex'}
              ml={2}
              alignItems="center"
              className={isPrimary ? classes.selectedTab : classes.disabledTab}
            >
              {t('universeForm.primaryTab')}
            </Box>
            <Box
              flexShrink={1}
              display={'flex'}
              ml={2}
              mr={1}
              alignItems="center"
              className={!isPrimary ? classes.selectedTab : classes.disabledTab}
            >
              {t('universeForm.rrTab')}
            </Box>
            {/* show during new universe creation only */}
            {isNewUniverse && onDeleteRR && !!asyncFormData && (
              <YBButton
                className={classes.clearRRButton}
                component={Link}
                variant="ghost"
                size="small"
                data-testid="UniverseForm-ClearRR"
                onClick={onDeleteRR}
              >
                {t('universeForm.clearReadReplica')}
              </YBButton>
            )}
          </>
        )}
      </>
    );
  };

  const renderFooter = () => {
    return (
      <>
        <Grid container justifyContent="space-between">
          <Grid item lg={8}>
            <Box
              width="100%"
              display="flex"
              flexShrink={1}
              justifyContent="flex-start"
              alignItems="center"
            >
              <UniverseResourceContainer data={universeResourceTemplate} />
            </Box>
          </Grid>
          <Grid item lg={4}>
            <Box width="100%" display="flex" justifyContent="flex-end">
              <YBButton
                variant="secondary"
                size="large"
                onClick={onCancel}
                data-testid="UniverseForm-Cancel"
              >
                {t('common.cancel')}
              </YBButton>
              &nbsp;
              {/* shown only during create primary + RR flow ( fresh universe ) */}
              {onClusterTypeChange && (
                <YBButton
                  variant="secondary"
                  size="large"
                  onClick={handleClusterChange}
                  data-testid="UniverseForm-ClusterChange"
                >
                  {isPrimary
                    ? t('universeForm.actions.configureRR')
                    : t('universeForm.actions.backPrimary')}
                </YBButton>
              )}
              {/* shown only during edit RR flow */}
              {onDeleteRR && isEditRR && (
                <YBButton
                  variant="secondary"
                  size="large"
                  onClick={onDeleteRR}
                  data-testid="UniverseForm-DeleteRR"
                >
                  {t('universeForm.actions.deleteRR')}
                </YBButton>
              )}
              &nbsp;
              <YBButton
                variant="primary"
                size="large"
                type="submit"
                data-testid="UniverseForm-Submit"
              >
                {submitLabel ? submitLabel : t('common.save')}
              </YBButton>
            </Box>
          </Grid>
        </Grid>
      </>
    );
  };

  const renderSections = () => {
    return (
      <>
        <CloudConfiguration />
        <InstanceConfiguration />
        <AdvancedConfiguration />
        <GFlags />
        <UserTags />
        <HelmOverrides />
      </>
    );
  };

  //Form Context Values
  // const isPrimary = [clusterModes.NEW_PRIMARY, clusterModes.EDIT_PRIMARY].includes(mode);

  return (
    <Box className={classes.mainConatiner}>
      <FormProvider {...formMethods}>
        <form key={clusterType} onSubmit={formMethods.handleSubmit(onSubmit)}>
          <Box className={classes.formHeader}>{renderHeader()}</Box>
          <Box className={classes.formContainer}>{renderSections()}</Box>
          <Box className={classes.formFooter} mt={4}>
            {renderFooter()}
          </Box>
        </form>
      </FormProvider>
    </Box>
  );
};
