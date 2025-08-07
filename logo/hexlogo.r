
library(ggplot2)
library(magick)

ggplot() + 
  geom_polygon(
    mapping = aes(
      x = 1300 * sqrt(3)/2 * c(0, 1, 1, 0, -1, -1), 
      y = 1300 * .5 * c(2, 1, -1, -2, -1, 1) ),
    color     = '#0078a5',
    fill      = 'white',
    linewidth = 6 ) + 
  coord_fixed(ratio = 1) +
  theme_void() +
  theme(rect = element_rect(fill = 'transparent')) +
  annotate(
    geom     = 'text', 
    label    = 'jobqueue',
    family   = 'Titillium Web',
    fontface = 'bold',
    color    = '#181a1b',
    size     = 20,
    x        = 0, 
    y        = -550 ) +
  annotation_raster(
    raster = image_read('logo/cartoon.png'),
    xmin = -868, xmax = 868,
    ymin = -393, ymax = 993 )



ggsave(
  path     = 'logo',
  filename = 'jobqueue.png', 
  device   = 'png',
  width    = 2600, 
  height   = 2600, 
  dpi      = 400, 
  units    = 'px',
  bg       = 'transparent' )


image_read('logo/jobqueue.png') |>
  image_trim() |>
  image_resize('x200') |>
  image_write('man/figures/logo.png')
