


if __name__ == '__main__':
    sb = ScriptBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        parser_desc="Run data analysis of Stackoverflow job posts.",
        parser_formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    sb.parse_args()
    logger = sb.get_logger()
    try:
        logger.info("Starting the job data analysis")
        ja.JobDataAnalyzer(
            main_cfg_path=sb.args.main_cfg,
            logging_cfg=sb.logging_cfg_dict).run_analysis()
    except KeyboardInterrupt as e:
        logger.critical(e)
        sys.exit(1)
    else:
        logger.info("End of the whole job data analysis")
        logger.info("Program will exit")
        sys.exit(0)
