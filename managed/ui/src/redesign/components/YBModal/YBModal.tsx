import React, { FC, ReactElement } from 'react';
import clsx from 'clsx';
import {
  makeStyles,
  Theme,
  Dialog,
  DialogProps,
  DialogActions,
  DialogContent,
  DialogTitle,
  Typography,
  Slide,
  DialogContentProps
} from '@material-ui/core';
import type { TransitionProps } from '@material-ui/core/transitions';
import { YBTooltip, YBButton, YBButtonProps } from '../../components/';

export interface OverrideButtonProps {
  primary?: YBButtonProps;
  secondary?: YBButtonProps;
}

export interface YBModalProps extends DialogProps {
  title?: string;
  size?: 'xs' | 'sm' | 'md' | 'lg' | 'xl';
  overrideHeight?: string | number;
  overrideWidth?: string | number;
  isSidePanel?: boolean;
  titleSeparator?: boolean;
  titleIcon?: React.ReactNode;
  actionsInfo?: React.ReactNode;
  onClose?: () => void;
  onSubmit?: () => void;
  enableBackdropDismiss?: boolean;
  submitLabel?: string;
  submitButtonTooltip?: string;
  submitTestId?: string;
  cancelLabel?: React.ReactNode;
  cancelButtonTooltip?: string;
  cancelTestId?: string;
  buttonProps?: OverrideButtonProps;
  customTitle?: React.ReactNode;
  hideCloseBtn?: boolean;
  dialogContentProps?: DialogContentProps;
}

export const SlideTransition = React.forwardRef(
  (
    props: TransitionProps & { children?: React.ReactElement<unknown> },
    ref: React.Ref<unknown>
  ) => {
    return <Slide direction="left" ref={ref} {...props} />;
  }
);
SlideTransition.displayName = 'SlideTransition';

const useStyles = makeStyles<Theme, Partial<YBModalProps>>((theme) => ({
  dialogSm: {
    width: ({ overrideWidth }) => overrideWidth ?? 608,
    height: ({ overrideHeight }) => overrideHeight ?? 400
  },
  dialogMd: {
    width: ({ overrideWidth }) => overrideWidth ?? 800,
    height: ({ overrideHeight }) => overrideHeight ?? 600
  },
  dialogXs: {
    width: ({ overrideWidth }) => overrideWidth ?? 480,
    height: ({ overrideHeight }) => overrideHeight ?? 272
  },
  dialogXl: {
    width: ({ overrideWidth }) => overrideWidth ?? 800,
    height: ({ overrideHeight }) => overrideHeight ?? 800
  },
  form: {
    display: 'flex',
    flexDirection: 'column',
    margin: 0,
    minHeight: '100%'
  },
  sidePanel: {
    width: 736,
    height: '100%',
    maxHeight: '100%',
    margin: '0 0 0 auto',
    borderRadius: '0 !important',
    overflowY: 'unset'
  },
  dialogTitle: {
    padding: theme.spacing(2, 1.5, 2, 2)
  },
  dialogTitleXs: {
    padding: theme.spacing(1.5, 1.5, 1.5, 2)
  },
  closeBtn: {
    padding: theme.spacing(0.5),
    height: theme.spacing(4),
    float: 'right',
    margin: 'auto 0 auto auto',
    cursor: 'pointer'
  },
  closeBtnText: {
    color: theme.palette.orange[500],
    fontSize: theme.spacing(4),
    lineHeight: '26px'
  },
  actionsInfo: {
    marginRight: 'auto'
  },
  title: {
    display: 'flex'
  },
  dialogTitleSeparator: {
    borderBottom: `1px solid ${theme.palette.grey[200]}`
  },
  modalTitle: {
    display: 'flex',
    alignItems: 'center'
  },
  text: {
    marginLeft: theme.spacing(1),
    whiteSpace: 'nowrap'
  }
}));

export const YBModal: FC<YBModalProps> = (props: YBModalProps) => {
  const {
    title,
    titleSeparator,
    titleIcon,
    actionsInfo,
    onClose,
    onSubmit,
    children,
    cancelLabel,
    cancelTestId,
    submitLabel,
    submitTestId,
    enableBackdropDismiss,
    size,
    isSidePanel,
    buttonProps,
    customTitle,
    hideCloseBtn,
    submitButtonTooltip,
    cancelButtonTooltip,
    dialogContentProps = {},
    ...dialogProps
  } = props;
  const classes = useStyles(props);

  const handleClose = () => {
    if (onClose) {
      onClose();
    }
  };

  const handleSubmit = (event: React.MouseEvent) => {
    if (onSubmit) {
      onSubmit();
    }
    // Prevent event propagation to avoid unintended page refresh
    event.preventDefault();
  };

  delete dialogProps.overrideHeight; // Override only used in styles
  delete dialogProps.overrideWidth; // Override only used in styles

  let dialogClasses = classes.dialogMd;
  let dialogTitle = classes.dialogTitle;
  const showDialogActions = cancelLabel ?? submitLabel;

  switch (size) {
    case 'sm':
      dialogClasses = classes.dialogSm;
      break;
    case 'xs':
      dialogClasses = classes.dialogXs;
      dialogTitle = classes.dialogTitleXs;
      break;
    case 'xl':
      dialogClasses = classes.dialogXl;
      break;
    case 'md':
    default:
      dialogClasses = classes.dialogMd;
  }
  dialogTitle = clsx(dialogTitle, titleSeparator && classes.dialogTitleSeparator);
  if (isSidePanel) {
    dialogClasses = classes.sidePanel;
    dialogProps.TransitionComponent = SlideTransition;
  }

  const submitBtnProps = buttonProps?.primary ?? {};
  const cancelBtnProps = buttonProps?.secondary ?? {};
  if (!dialogProps.fullScreen) {
    dialogProps.classes = {
      paper: dialogClasses
    };
    dialogProps.fullWidth = true;
    dialogProps.maxWidth = size;
  }

  const getCancelButton = (): ReactElement | undefined => {
    const returnComponent: ReactElement | undefined = cancelLabel ? (
      <YBButton
        variant="secondary"
        onClick={handleClose}
        {...cancelBtnProps}
        data-testid={cancelTestId}
      >
        {cancelLabel}
      </YBButton>
    ) : undefined;

    if (cancelButtonTooltip) {
      return (
        <YBTooltip title={cancelButtonTooltip}>
          <span>{returnComponent}</span>
        </YBTooltip>
      );
    }
    return returnComponent;
  };

  const getSubmitButton = (): ReactElement | undefined => {
    const returnComponent: ReactElement | undefined = submitLabel ? (
      <YBButton
        variant="primary"
        onClick={handleSubmit}
        {...submitBtnProps}
        type="submit"
        autoFocus
        data-testid={submitTestId}
      >
        {submitLabel}
      </YBButton>
    ) : undefined;

    if (submitButtonTooltip) {
      return (
        <YBTooltip title={submitButtonTooltip}>
          <span>{returnComponent}</span>
        </YBTooltip>
      );
    }
    return returnComponent;
  };

  return (
    <Dialog
      aria-labelledby="form-dialog-title"
      {...dialogProps}
      onClose={enableBackdropDismiss ? handleClose : undefined}
      disableEnforceFocus
    >
      <form className={classes.form}>
        {customTitle ? (
          <div className={classes.modalTitle}>{customTitle}</div>
        ) : (
          title && (
            <DialogTitle id="form-dialog-title" disableTypography className={dialogTitle}>
              <Typography className={classes.title} variant="h4">
                {titleIcon ? (
                  <div className={classes.modalTitle}>
                    {titleIcon}
                    <span className={classes.text}>{title}</span>
                  </div>
                ) : (
                  <div className={classes.modalTitle}>{title}</div>
                )}
                {!hideCloseBtn && (
                  <YBButton className={classes.closeBtn} onClick={handleClose}>
                    <span className={classes.closeBtnText}>x</span>
                  </YBButton>
                )}
              </Typography>
            </DialogTitle>
          )
        )}
        <DialogContent {...dialogContentProps}>{children}</DialogContent>
        {showDialogActions && (
          <DialogActions>
            {actionsInfo && <div className={classes.actionsInfo}>{actionsInfo}</div>}
            {getCancelButton()}
            {getSubmitButton()}
          </DialogActions>
        )}
      </form>
    </Dialog>
  );
};

export type YBSidePanelProps = Omit<YBModalProps, 'isSidePanel'>;

export const YBSidePanel: FC<YBSidePanelProps> = (props) => (
  <YBModal {...props} isSidePanel={true} />
);
