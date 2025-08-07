
library(ggplot2)

ggplot() + 
  ggpattern::geom_polygon_pattern(
    mapping = aes(
      x = 1200 * sqrt(3)/2 * c(0, 1, 1, 0, -1, -1), 
      y = 1200 * .5 * c(2, 1, -1, -2, -1, 1) ),
    pattern          = 'image',
    pattern_type     = 'fit',
    pattern_filename = 'logo/cartoon.png',
    color            = 'black',
    fill             = 'white',
    linewidth        = 4 ) + 
  coord_fixed(ratio = 1) +
  theme_void() +
  theme(rect = element_rect(fill = 'transparent')) +
  annotate(
    geom   = 'text', 
    label  = 'jobqueue',
    family = 'Brownland',
    size   = 20,
    x      = 0, 
    y      = -580 )



ggsave(
  path     = 'logo',
  filename = 'interprocess.png', 
  device   = 'png',
  width    = 2000, 
  height   = 2000, 
  dpi      = 380, 
  units    = 'px',
  bg       = 'transparent' )


library(magick)
library(magrittr)

# pkgdown website sets logo width to 120px
image_read('logo/jobqueue.png') %>%
  image_trim() %>%
  image_resize('120x') %>%
  image_write('man/figures/logo.png')


# height = 200px (150pt) for joss paper
image_read('logo/jobqueue.png') %>%
  image_trim() %>%
  image_resize('x200') %>%
  image_write('joss/figures/logo.png')
