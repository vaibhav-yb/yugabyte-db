import type {
  CommonColors,
  PaletteColor,
  TypeBackground,
  YBAColors
} from '@material-ui/core/styles/createPalette';

export const colors = {
  primary: {
    100: '#F2F6FF',
    200: '#E5EDFF',
    300: '#CBDBFF',
    400: '#8DABF0',
    500: '#507CE1',
    600: '#2B59C3',
    700: '#1A44A5',
    800: '#0A2972',
    900: '#031541'
  } as PaletteColor,
  secondary: {
    100: '#F1F1F7',
    200: '#E9E9F2',
    300: '#CAC9DF',
    400: '#AAAAD0',
    500: '#7B7BB8',
    600: '#4F4FA4',
    700: '#30307F',
    800: '#171755',
    900: '#000041'
  } as PaletteColor,
  grey: {
    100: '#F0F4F7',
    200: '#E9EEF2',
    300: '#D7DEE4', // sometimes refered as #D7DDE1 in designs
    400: '#B7C3CB',
    500: '#97A5B0',
    600: '#6D7C88',
    700: '#4E5F6D',
    800: '#25323D',
    900: '#0B1117'
  } as PaletteColor,
  error: {
    100: '#FDE2E2',
    300: '#F9ACAC',
    500: '#DA1515',
    700: '#8F0000',
    900: '#590000'
  } as PaletteColor,
  warning: {
    100: '#FFEEC8',
    300: '#FFD383',
    500: '#FFA400',
    700: '#C88900',
    900: '#9D6C00'
  } as PaletteColor,
  success: {
    100: '#CDEFE1',
    300: '#82D2B0',
    500: '#13A868',
    700: '#097345',
    900: '#024126'
  } as PaletteColor,
  info: {
    100: '#D7EFF4',
    200: '#DFF5FF',
    300: '#9EE7F5',
    400: '#F8FBFC',
    500: '#45C8E2',
    700: '#00819B',
    900: '#003E4B'
  } as PaletteColor,
  orange: {
    100: '#F6AB91',
    300: '#FF6E42',
    500: '#EF5824',
    700: '#DC4E1D',
    900: '#A73D19'
  } as PaletteColor,
  background: {
    default: '#F7FAFC',
    paper: '#FFFFFF'
  } as TypeBackground,
  // non-semantic common colors for things like charts, progress bars, etc
  common: {
    black: '#000000',
    white: '#FFFFFF',
    blue: '#36B8F5',
    magenta: '#D74FEE',
    purple: '#BB43BC',
    cyan: '#43BFC2',
    orange: '#FF6E42',
    yellow: '#FFFAC8',
    indigo: '#5E60F0'
  } as CommonColors,
  chartStroke: {
    cat1: '#30307F',
    cat2: '#36B8F5',
    cat3: '#BB43BC',
    cat4: '#43BFC2',
    cat5: '#90948E',
    cat6: '#1C7180',
    cat7: '#EEA95F',
    cat8: '#3590D9',
    cat9: '#F0679E',
    cat10: '#707B8E'
  },
  chartFill: {
    area1: '#EAEAF2',
    area2: '#EBF8FE',
    area3: '#F8ECF8',
    area4: '#ECF8F9',
    area5: '#F4F4F3',
    area6: '#E8F1F2',
    area7: '#FDF6EF',
    area8: '#EBF4FB',
    area9: '#FDF0F5',
    area10: '#F0F2F3',

    // Bar chart properties
    bar1: '#0098F0',
    bar2: '#262666'
  },
  ybacolors: {
    ybOrangeFocus: '#EF582480',
    ybGray: '#DEDEE0',
    ybGrayHover: '#E5E5E9',
    ybDarkGray: '#232329',
    ybDarkGray1: '#9F9EA7',
    ybDarkGray2: '#D9D9DB',
    inputBackground: '#E6E6E6',
    backgroundDisabled: '#EEE',
    colorDisabled: '#555',
    darkBlue: '#303a78',
    inputBoxShadow: 'inset 0 1px 1px rgb(0 0 0 / 8%), 0 0 8px rgb(239 88 36 / 20%)'
  } as YBAColors
};

export const themeVariables = {
  screenMinWidth: 1024,
  screenMinHeight: 400,
  sidebarWidthMin: 62,
  sidebarWidthMax: 232,
  footerHeight: 40,
  toolbarHeight: 55,
  inputHeight: 42,
  borderRadius: 8,
  shadowLight: '0 0 4px 0 rgba(0,0,0,0.1)',
  shadowThick: '0 0 8px 0 rgba(0,0,0,0.1)'
};
