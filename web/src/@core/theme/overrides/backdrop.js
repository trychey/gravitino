/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

const Backdrop = () => {
  return {
    MuiBackdrop: {
      styleOverrides: {
        root: ({ theme }) => ({
          backgroundColor:
            theme.palette.mode === 'light' ? `rgba(${theme.palette.customColors.main}, 0.5)` : 'rgba(14, 15, 36, 0.68)'
        }),
        invisible: {
          backgroundColor: 'transparent'
        }
      }
    }
  }
}

export default Backdrop
