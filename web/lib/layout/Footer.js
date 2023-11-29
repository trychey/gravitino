/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import Link from 'next/link'

import { Box, Typography } from '@mui/material'

const Footer = props => {
  return (
    <Box component={'footer'} className={'layout-footer  twc-z-10 twc-flex twc-items-center twc-justify-center'}>
      <Box className='footer-content-wrapper twc-px-6 twc-w-full twc-py-[0.75rem] [@media(min-width:1440px)]:twc-max-w-[1440px]'>
        <Box className={'twc-flex twc-flex-wrap twc-items-center twc-justify-between'}>
          <Typography className='twc-mr-2'>
            {`© ${new Date().getFullYear()} `}
            <Link className={'twc-no-underline twc-text-primary-main'} target='_blank' href='https://datastrato.ai/'>
              Datastrato
            </Link>
          </Typography>
          <Box className={'twc-flex twc-flex-wrap twc-items-center [&>:not(:last-child)]:twc-mr-4'}>
            <Link
              className={'twc-no-underline twc-text-primary-main'}
              target='_blank'
              href='https://github.com/datastrato/gravitino/blob/main/LICENSE'
            >
              License
            </Link>
            <Link
              className={'twc-no-underline twc-text-primary-main'}
              target='_blank'
              href='https://datastrato.ai/docs/'
            >
              Documentation
            </Link>
            <Link
              className={'twc-no-underline twc-text-primary-main'}
              target='_blank'
              href='https://github.com/datastrato/gravitino/issues'
            >
              Support
            </Link>
          </Box>
        </Box>
      </Box>
    </Box>
  )
}

export default Footer
